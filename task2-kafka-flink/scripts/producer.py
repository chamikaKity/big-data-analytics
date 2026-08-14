"""Streams Austin Camera Traffic Counts CSV rows into the traffic-telemetry Kafka topic.

Rows are sorted by read_date ascending and published one every INTERVAL_SECONDS,
so downstream Flink event-time watermarks (2.3) advance monotonically.
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "camera_traffic_counts.csv"
TOPIC_PARTITIONS = 3
TOPIC_REPLICATION_FACTOR = 1


def ensure_topic(bootstrap_servers: str, topic: str) -> None:
    """Create the topic with the required partition count if it doesn't exist yet.

    Kafka's auto.create.topics.enable defaults to true, which would otherwise
    silently create the topic with 1 partition on first produce() instead of
    the 3 required by the spec.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    if topic in admin.list_topics(timeout=10).topics:
        return
    new_topic = NewTopic(topic, num_partitions=TOPIC_PARTITIONS, replication_factor=TOPIC_REPLICATION_FACTOR)
    futures = admin.create_topics([new_topic])
    for created_topic, future in futures.items():
        try:
            future.result()
            print(f"created topic '{created_topic}' ({TOPIC_PARTITIONS} partitions, RF {TOPIC_REPLICATION_FACTOR})")
        except KafkaException as e:
            if "already exists" not in str(e):
                raise


def build_payload(row: dict) -> dict:
    return {
        "record_id": row["record_id"],
        "atd_device_id": row["atd_device_id"],
        "read_date": row["read_date"],
        "intersection_name": row["intersection_name"],
        "direction": row["direction"],
        "movement": row["movement"],
        "heavy_vehicle": row["heavy_vehicle"].lower() == "true",
        "volume": int(row["volume"]),
        "speed_average": float(row["speed_average"]) if row["speed_average"] else None,
        "bin_duration": int(row["bin_duration"]),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="traffic-telemetry")
    parser.add_argument("--csv-path", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=None, help="stop after N messages (default: all rows)")
    args = parser.parse_args()

    with args.csv_path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    rows.sort(key=lambda r: r["read_date"])
    if args.limit:
        rows = rows[: args.limit]

    ensure_topic(args.bootstrap_servers, args.topic)
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    print(f"streaming {len(rows)} rows to '{args.topic}' every {args.interval_seconds}s")
    for i, row in enumerate(rows, start=1):
        payload = build_payload(row)
        producer.produce(
            args.topic,
            key=payload["atd_device_id"],
            value=json.dumps(payload),
            callback=delivery_report,
        )
        producer.poll(0)
        if i % 50 == 0 or i == len(rows):
            print(f"  sent {i}/{len(rows)} (last read_date={payload['read_date']})")
        time.sleep(args.interval_seconds)

    producer.flush()
    print("done")


if __name__ == "__main__":
    main()
