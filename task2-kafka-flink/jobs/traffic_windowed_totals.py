"""Reads traffic-telemetry from Kafka, watermarks on event time, and emits a
10-minute tumbling-window total vehicle count per sensor (atd_device_id).
"""

import json
from datetime import datetime

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows, Time

KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
SOURCE_TOPIC = "traffic-telemetry"
CONSUMER_GROUP = "flink-traffic-windowed-totals"


def parse_event_time_millis(read_date: str) -> int:
    return int(datetime.fromisoformat(read_date).timestamp() * 1000)


class ReadDateTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return parse_event_time_millis(json.loads(value)["read_date"])


def to_sensor_volume(json_str: str):
    row = json.loads(json_str)
    return row["atd_device_id"], row["volume"]


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # withIdleness: some Kafka partitions may see no traffic at all (e.g. a
    # sensor key never hashes onto them), and an empty split's watermark
    # never advances - without this, that single empty partition permanently
    # blocks the combined watermark (Flink takes the min across all splits),
    # so no window ever fires even though other partitions have plenty of data.
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(ReadDateTimestampAssigner())
        .with_idleness(Duration.of_seconds(20))
    )

    stream = env.from_source(source, watermark_strategy, "traffic-telemetry-source")

    (
        stream.map(to_sensor_volume, output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
        .key_by(lambda sensor_volume: sensor_volume[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(10)))
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
        .map(
            lambda sensor_total: f"sensor={sensor_total[0]} window_total_volume={sensor_total[1]}",
            output_type=Types.STRING(),
        )
        .print()
    )

    env.execute("traffic-windowed-totals")


if __name__ == "__main__":
    main()
