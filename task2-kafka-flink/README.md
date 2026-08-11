# Task 2: Real-Time Stream Ingestion & Processing with Apache Kafka and Apache Flink

**Objective:** Stand up Kafka (KRaft or ZooKeeper) + Flink JobManager/TaskManager in Docker,
produce Austin traffic camera data into a 3-partition `traffic-telemetry` topic every 2s, and run a
PyFlink/Java job with bounded-out-of-orderness watermarking (10s skew) and a 10-minute
tumbling window aggregating vehicle counts per sensor.

**Dataset:** https://data.austintexas.gov/Transportation-and-Mobility/Camera-Traffic-Counts/sh59-i6y9/about_data

**Status:** not started
