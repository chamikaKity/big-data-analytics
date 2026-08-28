# Task 2: Real-Time Stream Ingestion & Processing with Apache Kafka and Apache Flink

**Objective:** Stand up Kafka (KRaft or ZooKeeper) + Flink JobManager/TaskManager in Docker,
produce Austin traffic camera data into a 3-partition `traffic-telemetry` topic every 2s, and run a
PyFlink/Java job with bounded-out-of-orderness watermarking (10s skew) and a tumbling
window aggregating vehicle counts per sensor. **Window size: 15 minutes, not the coursework
brief's stated 10 minutes — see the 2.3 note below for why.**

**Dataset:** https://data.austintexas.gov/Transportation-and-Mobility/Camera-Traffic-Counts/sh59-i6y9/about_data

**Status:** 2.1 done — `docker-compose.yml` brings up Kafka (KRaft, single node, apache/kafka:3.8.0)
+ Flink JobManager/TaskManager (flink:1.19.1-scala_2.12-java11) on a shared `bigdata-net` network.
Verified: Kafka topic create/list/delete via `kafka-topics.sh`, Flink dashboard at localhost:8081
shows 1 TaskManager registered with 2 slots.

2.2 done — `traffic-telemetry` topic created (3 partitions, RF 1). Dataset: 5000 most recent rows
of the Austin Camera Traffic Counts Socrata dataset (`data.austintexas.gov/resource/sh59-i6y9`),
fetched via `$order=read_date DESC&$limit=5000` into `data/camera_traffic_counts.csv`
(~951KB, 7 sensors, 2024-07-08 12:30–23:45). `scripts/producer.py` (uv project, `confluent-kafka`)
sorts rows by `read_date` ascending, publishes one JSON message every 2s keyed by `atd_device_id`.
Verified via console consumer that messages land correctly.

**Committed to git, unlike the other tasks' datasets:** every other task's dataset is gitignored
and re-fetched via a documented command (see Task 1's README) because the underlying source is a
fixed historical archive - re-running the fetch reproduces the exact same data. This dataset isn't
that: Socrata's `Camera Traffic Counts` is a live, continuously growing table, so
`$order=read_date DESC&$limit=5000` pulls a *different* 5000 rows every time it's re-run (whatever
is newest "now"). Re-fetching later would not reproduce this exact dataset, so
`data/camera_traffic_counts.csv` is committed directly (see `.gitignore`'s explicit exception for
this one file) to keep the producer's output reproducible.

2.3 done — `jobs/traffic_windowed_totals.py`, submitted via `flink run -d -py` (Flink CLI, inside
the jobmanager container, satisfying "deploy via Flink dashboard/CLI"). Uses the **Table API /
SQL**, not the DataStream API — see the dedicated note below for why. Declares a Kafka source
table over `traffic-telemetry` with `WATERMARK FOR read_date AS read_date - INTERVAL '10' SECOND`
(the required 10s bounded-out-of-orderness), aggregates with the `TUMBLE` windowing table-valued
function (15 minutes) grouped by `atd_device_id`, `SUM(volume)`, and inserts into a `print`-connector
sink table.

**Window size deviation (10min → 15min), deliberate, documented here per the assignment's own
substitution-notes convention:** the Camera Traffic Counts source data is itself pre-aggregated
into 15-minute bins (`bin_duration=900` on every row; all rows sharing a given `read_date`
share the *exact same* timestamp, with no spread within the bin). A 10-minute window doesn't
divide evenly into a 15-minute cadence, so consecutive window/bin boundaries drift out of phase:
over any 30-minute stretch, 2 of 3 ten-minute windows happen to catch a bin and fire, but the
3rd catches nothing and silently produces no output (not a `0` — `reduce` just never fires for
an empty window). That's not a bug, but it's an artificial, misleading gap purely from the
window size not matching the data's real granularity, not from any actual lull in traffic.
Switching the window to 15 minutes makes every window align with exactly one source bin, so
window totals map 1:1 onto real 15-minute observation periods with no dropped windows —
a materially more meaningful aggregation for this dataset than the brief's stated 10 minutes.

The stock `flink:1.19.1-scala_2.12-java11` image has no Python/PyFlink/Kafka connector, so
`flink-python/Dockerfile` extends it: installs Python3 + a full JDK (pemja, PyFlink's JNI bridge,
needs `jni.h`/`Python.h` headers to compile, which the base image's JRE-only `/opt/java/openjdk`
lacks) + gcc, `pip install apache-flink==1.19.2`, and downloads
`flink-sql-connector-kafka-3.3.0-1.19.jar` into `/opt/flink/lib/`. `docker-compose.yml`'s
jobmanager/taskmanager now `build: ./flink-python` instead of pulling the stock image, with
`./jobs` mounted into both containers at `/opt/flink/jobs`.

**Watermark-idleness gotcha, worth knowing for the viva:** with 7 sensor IDs hashed across 3
Kafka partitions, one partition ended up receiving zero messages. Flink combines watermarks
across all partitions by taking the *minimum*, so that one empty partition permanently stalls
the watermark for the whole job unless told otherwise — `numRecordsIn` on the window operator
was healthy but `numRecordsOut` stayed at 0 forever, no window ever fired. Fixed with the Table
API's `'scan.watermark.idle-timeout' = '20s'` connector option, which excludes partitions with
no data for 20s from the watermark computation (the DataStream-API equivalent is
`WatermarkStrategy.withIdleness(...)`).

**DataStream API → Table API rewrite, worth knowing for the viva:** the first working version of
this job used the DataStream Python API (`KafkaSource` → `.map()` → `.key_by()` →
`.window(TumblingEventTimeWindows)` → `.reduce()` → `.print()`), same idleness fix as above. It
looked correct and initially *appeared* to work — but under close, timed re-testing (fresh topic,
pre-loaded batch of 450 records spanning 5 distinct 15-minute bins, isolated down to a
single-partition topic with parallelism 1 to rule out the idleness/partition-skew angle entirely)
the window genuinely never fired, sometimes for 15+ minutes of real job runtime with healthy
`numRecordsIn` and zero `numRecordsOut` throughout. Root cause: PyFlink's DataStream Python
UDFs (the `.map()`/`.reduce()` lambdas) execute out-of-process through Apache Beam's Python
portability layer (`pemja`, the JNI bridge `flink-python/Dockerfile` compiles from source, is
what connects the JVM to that separate Python worker) — watermark propagation across that
process boundary proved unreliable. Rewriting the same logic as pure SQL/Table API (shown above)
avoids Python UDFs entirely for this job — `SUM()` and `TUMBLE()` compile to native JVM
execution — and it fired correctly within seconds of real load. Verified output (`window_start`,
`window_end`, `atd_device_id`, `window_total_volume`), correctly aligned 1:1 to the source's real
15-minute bins with no gaps:
```
+I[2024-07-08T12:30, 2024-07-08T12:45, 6653, 313]
+I[2024-07-08T12:45, 2024-07-08T13:00, 6882, 350]
+I[2024-07-08T12:45, 2024-07-08T13:00, 7038, 788]
+I[2024-07-08T13:00, 2024-07-08T13:15, 6882, 320]
+I[2024-07-08T12:45, 2024-07-08T13:00, 6808, 359]
+I[2024-07-08T12:45, 2024-07-08T13:00, 6653, 523]
+I[2024-07-08T12:45, 2024-07-08T13:00, 7343, 564]
+I[2024-07-08T12:45, 2024-07-08T13:00, 6382, 470]
+I[2024-07-08T12:45, 2024-07-08T13:00, 6881, 477]
+I[2024-07-08T13:00, 2024-07-08T13:15, 6808, 337]
+I[2024-07-08T13:00, 2024-07-08T13:15, 6382, 668]
+I[2024-07-08T13:00, 2024-07-08T13:15, 6881, 451]
+I[2024-07-08T13:00, 2024-07-08T13:15, 7038, 782]
+I[2024-07-08T13:00, 2024-07-08T13:15, 6653, 469]
+I[2024-07-08T13:00, 2024-07-08T13:15, 7343, 536]
```
(visible via `docker logs taskmanager`; `+I` = Flink's changelog "insert" row-kind marker, expected
for append-only windowed aggregates. Job also shows `RUNNING` on the Flink dashboard at
localhost:8081 while active.)
