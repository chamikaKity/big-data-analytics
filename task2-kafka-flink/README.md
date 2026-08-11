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
Verified via console consumer that messages land correctly. Next: 2.3 (PyFlink windowed job).
