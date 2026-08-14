# Task 2: Real-Time Stream Ingestion & Processing with Apache Kafka and Apache Flink

**Objective:** Stand up Kafka (KRaft or ZooKeeper) + Flink JobManager/TaskManager in Docker,
produce Austin traffic camera data into a 3-partition `traffic-telemetry` topic every 2s, and run a
PyFlink/Java job with bounded-out-of-orderness watermarking (10s skew) and a 10-minute
tumbling window aggregating vehicle counts per sensor.

**Dataset:** https://data.austintexas.gov/Transportation-and-Mobility/Camera-Traffic-Counts/sh59-i6y9/about_data

**Status:** 2.1 done — `docker-compose.yml` brings up Kafka (KRaft, single node, apache/kafka:3.8.0)
+ Flink JobManager/TaskManager (flink:1.19.1-scala_2.12-java11) on a shared `bigdata-net` network.
Verified: Kafka topic create/list/delete via `kafka-topics.sh`, Flink dashboard at localhost:8081
shows 1 TaskManager registered with 2 slots.

2.2 done — `traffic-telemetry` topic created (3 partitions, RF 1). Dataset: 5000 most recent rows
of the Austin Camera Traffic Counts Socrata dataset (`data.austintexas.gov/resource/sh59-i6y9`),
fetched via `$order=read_date DESC&$limit=5000` into `data/camera_traffic_counts.csv` (gitignored,
~951KB, 7 sensors, 2024-07-08 12:30–23:45). `scripts/producer.py` (uv project, `confluent-kafka`)
sorts rows by `read_date` ascending, publishes one JSON message every 2s keyed by `atd_device_id`.
Verified via console consumer that messages land correctly.

2.3 done — `jobs/traffic_windowed_totals.py`, submitted via `flink run -d -py` (Flink CLI, inside
the jobmanager container, satisfying "deploy via Flink dashboard/CLI"). Reads `traffic-telemetry`
via `KafkaSource`, uses `WatermarkStrategy.for_bounded_out_of_orderness(10s)` with the timestamp
assigner parsing each row's `read_date` field as event time, keys by `atd_device_id`, aggregates
with a 10-minute `TumblingEventTimeWindows` + `reduce` summing `volume`, prints
`sensor=<id> window_total_volume=<n>` per window.

The stock `flink:1.19.1-scala_2.12-java11` image has no Python/PyFlink/Kafka connector, so
`flink-python/Dockerfile` extends it: installs Python3 + a full JDK (pemja, PyFlink's JNI bridge,
needs `jni.h`/`Python.h` headers to compile, which the base image's JRE-only `/opt/java/openjdk`
lacks) + gcc, `pip install apache-flink==1.19.2`, and downloads
`flink-sql-connector-kafka-3.3.0-1.19.jar` into `/opt/flink/lib/`. `docker-compose.yml`'s
jobmanager/taskmanager now `build: ./flink-python` instead of pulling the stock image, with
`./jobs` mounted into both containers at `/opt/flink/jobs`.

**Watermark-idleness gotcha, worth knowing for the viva:** with 7 sensor IDs hashed across 3
Kafka partitions, one partition ended up receiving zero messages. Flink combines watermarks
across all partitions by taking the *minimum*, so that one empty partition permanently stalled
the watermark for the whole job — `numRecordsIn` on the window operator was healthy but
`numRecordsOut` stayed at 0 forever, no window ever fired. Fixed by adding
`.with_idleness(Duration.of_seconds(20))` to the watermark strategy, which excludes partitions
with no data for 20s from the watermark computation. Verified after the fix — streaming a burst
of rows spanning several `read_date` bins produced real window output:
```
sensor=6653 window_total_volume=81
sensor=6882 window_total_volume=350
sensor=6881 window_total_volume=7
sensor=6653 window_total_volume=836
sensor=7343 window_total_volume=564
sensor=7038 window_total_volume=788
sensor=6382 window_total_volume=470
sensor=6653 window_total_volume=307
```
(visible via `docker logs taskmanager`; job also shows `RUNNING` with 4 tasks on the Flink
dashboard at localhost:8081).
