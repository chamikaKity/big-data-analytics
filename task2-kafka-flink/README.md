# Task 2: Kafka + Flink

Stream Austin traffic camera data through Kafka, aggregate it in Flink with event-time windowing.

**Dataset:** [Camera Traffic Counts](https://data.austintexas.gov/Transportation-and-Mobility/Camera-Traffic-Counts/sh59-i6y9/about_data) (Austin, Socrata)

**Status:** 2.1 ✅ · 2.2 ✅ · 2.3 ✅ — see `RUNBOOK.md` for commands, this file is the design writeup.

## 2.1 — Infrastructure

- Kafka: KRaft mode, single node, `apache/kafka:3.8.0`
- Flink: JobManager + TaskManager, `flink:1.19.1-scala_2.12-java11`
- Shared Docker network `bigdata-net`
- Verified: topic create/list/delete works; dashboard (`localhost:8081`) shows 1 TaskManager, 2 slots

## 2.2 — Topic + Producer

- Topic `traffic-telemetry` — 3 partitions, replication factor 1
- Data: 5000 most recent rows from the Socrata API (~951KB, 7 sensors, 2024-07-08 12:30–23:45)
- `scripts/producer.py` — sorts by `read_date`, publishes JSON every 2s, keyed by `atd_device_id`
- Verified via console consumer

**Dataset is committed to git**, unlike the other tasks. Data source:
[Austin Camera Traffic Counts](https://data.austintexas.gov/Transportation-and-Mobility/Camera-Traffic-Counts/sh59-i6y9/about_data)
(Socrata) — historical and frozen, last updated 2024-07-08.

The 5000 rows come from `$order=read_date DESC&$limit=5000`, but thousands of rows share the same
`read_date`, so there's no unique tie-breaker — a re-fetch isn't guaranteed to return the exact
same rows. Committing the file avoids that (`.gitignore` has an explicit exception for it).

## 2.3 — Windowed Aggregation Job

`jobs/traffic_windowed_totals.py` — submitted via `flink run -d -py` (Flink CLI in the jobmanager
container).

| | |
|---|---|
| API | Table API / SQL (not DataStream — [why](#gotcha-datastream-api-silently-never-fired)) |
| Watermark | `read_date - INTERVAL '10' SECOND` (bounded-out-of-orderness) |
| Window | `TUMBLE`, 15 minutes ([not the brief's 10 — why](#gotcha-window-is-15-min-not-10)) |
| Aggregation | `SUM(volume)` grouped by `atd_device_id` |
| Output | `print` connector sink → TaskManager stdout |

Sample output (verified, correctly aligned to source bins, no gaps):
```
+I[2024-07-08T12:30, 2024-07-08T12:45, 6653, 313]
+I[2024-07-08T12:45, 2024-07-08T13:00, 6882, 350]
+I[2024-07-08T13:00, 2024-07-08T13:15, 6382, 668]
```
(`+I` = Flink's changelog "insert" marker, expected for append-only windowed aggregates)

### Custom Flink image

The stock Flink image is JVM-only — no Python, no PyFlink, no Kafka connector.
`flink-python/Dockerfile` adds:
- Python3 + a full JDK + gcc (needed to compile `pemja`, PyFlink's JNI bridge — the stock image's
  JRE has no `include/` headers)
- `apache-flink==1.19.2`
- `flink-sql-connector-kafka-3.3.0-1.19.jar`

`docker-compose.yml` builds from this instead of pulling the stock image; `./jobs` is mounted
into both containers at `/opt/flink/jobs`.

## Gotchas (viva notes)

#### Window is 15 min, not 10
The brief says 10 minutes. The source data is pre-aggregated into 15-minute bins
(`bin_duration=900`), and 10 doesn't divide evenly into 15 — 1 in every 3 ten-minute windows
lands on a gap between bins and silently produces nothing. Using 15-minute windows instead makes
every window map 1:1 onto a real observation bin, so nothing gets dropped.

#### Idle Kafka partition stalls the watermark
7 sensor IDs hashed across 3 partitions → one partition got zero messages. Flink computes the
job's watermark as the *minimum* across all partitions, so that one empty partition blocked
everything forever — data was flowing (`numRecordsIn` healthy) but no window ever closed
(`numRecordsOut` stuck at 0). Fix: `'scan.watermark.idle-timeout' = '20s'` on the source table,
which excludes silent partitions from the watermark calculation.

#### DataStream API silently never fired
The first working version used the DataStream Python API (`.map()` → `.key_by()` →
`.window()` → `.reduce()`). It looked correct, ran without errors, consumed data fine — but under
close testing (isolated to a single-partition topic, parallelism 1, to rule out the idle-partition
issue above) the window still never fired, even after 15+ minutes of real runtime.

**Root cause:** DataStream Python UDFs execute out-of-process through Apache Beam's Python worker
bridge (`pemja`, JNI). Watermark propagation across that process boundary was unreliable.
**Fix:** rewrote the same logic as pure SQL/Table API — `SUM()` and `TUMBLE()` compile to native
JVM execution, no Python UDF involved — and it fired correctly within seconds.
