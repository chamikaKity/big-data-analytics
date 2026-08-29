# Task 3: Scalable Data Analytics with Apache Spark

## Objective
Run a standalone Spark cluster in Docker (1 master, 2 workers capped at 2 cores/2GB each),
use PySpark to parse the SNAP `web-BerkStan` graph, compute the in-degree distribution, and
identify the top 50 destination nodes with caching/broadcast-join optimizations. Capture stage
durations, DAG structure, and shuffle/skew evidence from the Spark UI.

**Dataset:** [SNAP web-BerkStan](https://snap.stanford.edu/data/web-BerkStan.html) — 685,230
nodes, 7,600,595 directed edges. Each node is a crawled page from berkeley.edu or stanford.edu;
each directed edge is a hyperlink from one page to another, same-domain or cross-domain:

![BerkStan Hyperlink Graph](figures/berkstan_hyperlink_graph.png)

## 3.1 — Cluster
`docker-compose.yml` runs `apache/spark:3.5.9-python3` as one master (UI :8080, RPC :7077) and
two workers, each capped at 2 cores / 2GB via both Spark worker flags and Docker
`deploy.resources.limits`. A fourth `spark-history` service (port 18080) reads a shared
event-log volume (`data/spark-events/`, configured in `conf/spark-defaults.conf`) so completed
job metrics stay inspectable after the driver exits.

Confirmed via the master's REST endpoint — both workers registered ALIVE with 2 cores / 2.0 GiB
each (4 cores / 4GB total):

**Fig 1** — Spark master UI (`localhost:8080`) showing both workers ALIVE:
![Spark Master UI](figures/01_master_ui_workers.png)

## 3.2 — PySpark Job
`scripts/indegree_job.py`, submitted via `spark-submit --master spark://spark-master:7077`:
1. `spark.read.text()` lazily loads the raw edge list (no computation until an action runs).
2. Filters `#`-comment header lines, splits each row on `\s+` (the source file uses
   tab-separated values with trailing `\r`), casts to `(src, dst)` long columns.
3. `.cache()`s the parsed edges DataFrame since it's reused for both the in-degree aggregation
   and the broadcast join.
4. `groupBy("dst").count()` computes in-degree per destination vertex, `orderBy(desc).limit(50)`
   takes the top 50.
5. Broadcast-joins the (tiny) top-50 set back against the full edge list — `F.broadcast(hub)` —
   to count how many of the 7.6M edges land on a top-50 hub node, without shuffling the full
   edge table.

**Result:**

| Metric | Value |
|---|---|
| Total edges parsed | 7,600,595 |
| Distinct destination vertices | 617,094 |
| Top in-degree node | `438238` — in-degree 84,208 |
| Edges landing on a top-50 hub | 1,371,189 (~18% of all edges) |

Top 10 destination nodes by in-degree:

| dst | in_degree |
|---|---|
| 438238 | 84,208 |
| 401873 | 48,205 |
| 184094 | 44,290 |
| 768 | 44,101 |
| 927 | 44,067 |
| 184142 | 44,041 |
| 184279 | 44,037 |
| 184332 | 44,034 |
| 33 | 44,032 |
| 743 | 44,022 |

The full top-50 output is in
[`output/top50_indegree/top50_indegree.csv`](output/top50_indegree/top50_indegree.csv).

## 3.3 — Spark UI Evidence
Captured from the Spark History Server (`localhost:18080`) for application
`app-20260826025116-0000`, cross-checked against the REST API (`/api/v1/applications/.../stages`,
`.../executors`) to confirm the UI numbers.

### Stage durations & shuffle
The job resolved into 26 stages (many `SKIPPED` — reused from the cached `edges` DataFrame across
the repeated actions). Completed stages:

| Stage | Operation | Tasks | Exec time (s) | Input | Shuffle read | Shuffle write |
|---|---|---|---|---|---|---|
| 0 | text file scan (`show`) | 4 | 10.32 | 110.5 MB | — | — |
| 1 | filter/split/cast → shuffle write (groupBy map side) | 4 | 3.28 | 25.0 MB | — | 5.65 MB |
| 3 | groupBy reduce + sort (`show`) | 200 | 3.95 | — | 5.65 MB | 0.14 MB |
| 4 | limit merge | 1 | 0.02 | — | 0.14 MB | — |
| 11 | edges.count() (cached, re-scan) | 4 | 0.43 | 25.0 MB | — | — |
| 14 | broadcast join + count | 4 | 1.58 | 25.0 MB | — | 3.42 MB |
| 16 | join reduce-side count | 3 | 0.45 | — | 3.42 MB | — |
| 20 | in_degree.count() | 4 | 0.52 | 25.0 MB | — | — |
| 25 | write top-50 CSV | 1 | 0.06 | — | — | — |

Total application wall time: **12.5s** (start 02:51:16.008 → end 02:51:28.521 UTC).

**Fig 2** — History Server job timeline (13 jobs, one per Spark action — `show`, four `count`s,
the broadcast-join `count`, and the final `csv` write):
![Jobs Timeline](figures/02_jobs_timeline.png)

Most of the 13 jobs are trivial single-stage `count()` calls with no shuffle, so not worth a
screenshot. **Job 2 was chosen for Figs 3-5** because it's the one with an actual shuffle boundary
(3 total stages) — it's the `groupBy("dst")` → `orderBy(desc).limit(50)` computation, the core of
3.2, so its DAG and stage metrics are the most representative evidence of what this job actually
does.

**Fig 3** — Details for Job 2 (the `orderBy(desc).limit(50)` top-50 computation): SUCCEEDED in
3s, 2 completed stages + 1 skipped, with the executor-add event timeline:
![Job 2 Details](figures/03_job2_details.png)

**Fig 4** — Job 2's stage breakdown: Stage 3 (200/200 tasks, 5.4 MiB shuffle read, 137.1 KiB
shuffle write — the groupBy reduce-side sort) and Stage 4 (44 ms, 137.1 KiB shuffle read — final
merge) completed; Stage 2 (the cached `edges` scan) skipped:
![Job 2 Stages](figures/04_job2_stages_table.png)

**Fig 5** — Job 2's DAG for the same top-50 computation: Stage 2 (greyed out — the cached `edges`
scan, reused rather than recomputed), Stage 3 (`Exchange` — the shuffle for the global sort —
feeding `TakeOrderedAndProject`), and Stage 4 (final result collection):
![Stage DAG](figures/05_stage_dag.png)

### Partition skew evidence
The initial text-scan stage (stage 0, 4 tasks — one per core across the 2 workers) shows real
skew: Spark splits the 110MB input file by **byte offset**, not by line count, so each task reads
a similar number of bytes but a different number of complete records:

| Task | Executor host | Duration | Input | Records read |
|---|---|---|---|---|
| 0 | 192.168.155.5 | 3266 ms | 28.7 MB | 2,163,123 |
| 1 | 192.168.155.3 | 3076 ms | 28.7 MB | 1,905,925 |
| 2 | 192.168.155.5 | 3141 ms | 28.7 MB | 1,905,676 |
| 3 | 192.168.155.3 | 2936 ms | 24.4 MB | 1,625,875 |

Record count varies **33%** between the busiest and lightest task (2.16M vs 1.63M) purely from
line-length variation near block boundaries, even though byte-size per partition is near-uniform.

By contrast, the groupBy shuffle's 200 reduce-side tasks (stage 3) show almost **no** skew —
shuffle-read bytes ranged only 27.4KB–29.2KB (p5–p95), a ~6% spread — because Spark's default
hash partitioner distributes the (roughly uniformly distributed integer) `dst` node IDs evenly
across 200 partitions.

At the executor level, load stayed balanced across both workers — evidence the 2-core/2-worker
cap didn't starve either node:

| Executor | Host | Tasks | Input | Shuffle read | Shuffle write | Total task time |
|---|---|---|---|---|---|---|
| 0 | 192.168.155.3 | 115 | 102.1 MB | 3.89 MB | 4.66 MB | 13.4 s |
| 1 | 192.168.155.5 | 115 | 108.3 MB | 5.32 MB | 4.55 MB | 14.5 s |

**Fig 6** — History Server executors tab from a repeat run, confirming the same balanced-load
pattern (128/102 tasks, 103.3/97.4 MiB input split near-evenly across the two workers):
![Executors Tab](figures/06_executors_tab.png)

**Fig 7** — Stage 3 detail page, corroborating the low-skew claim above: across all 200 tasks,
shuffle-read size ranges only 26.4–29.3 KiB (Min–Max) and duration 4ms–0.2s, a tight spread
consistent with hash-partitioned `dst` IDs landing evenly across partitions:
![Stage 3 Task Skew](figures/07_stage3_task_skew.png)

### Tuning notes: shuffle partitions and skew mitigation
`spark.sql.shuffle.partitions` defaults to 200, and the job never overrides it. For this
aggregate that's over-provisioned: the groupBy reduces 7.6M edges down to 617,094 (dst, count)
pairs — spread across 200 reduce tasks, each handles only ~3,200 records and ~28 KB (Fig 7),
well below the point where a task's own scheduling overhead is worth paying to parallelize
further. A partition count closer to the total core count (4, or a small multiple like 8-16)
would cut scheduling overhead with no real loss of parallelism at this data size. 200 only starts
paying for itself at datasets with far more than 617K reduce-side keys.

The task-level evidence above (Fig 7) shows this dataset didn't need skew mitigation — hash
partitioning on `dst` already spread load evenly, and because `count()` is algebraic, Spark
map-side pre-aggregates before the shuffle, so even the busiest hub node (in-degree 84,208)
contributes one small partial count per source partition, not 84,208 raw rows. Skew becomes a
real problem in two cases this job doesn't hit: a `groupBy` on a non-associative aggregate (e.g.
`collect_list`) where no map-side combine is possible, or a join where a handful of hub keys
match a disproportionate share of rows on the other side (a join against `edges` keyed on a
popular `dst`, for instance, rather than the broadcast join used here). The standard fix in both
cases is **salting**: append a random suffix (`dst_salted = dst || '_' || rand(0, N)`) to spread
a hot key across N synthetic partitions for the shuffle, aggregate/join per salted key, then
strip the suffix and merge the N partial results in a second, much cheaper reduce pass. It
trades one extra shuffle stage for turning one overloaded reducer into N balanced ones — worth
it only when a real hot-key skew shows up in the task metrics, which is exactly the kind of
evidence Fig 7 provides for deciding it wasn't needed here.

## Limitations / what I'd do differently at scale
This whole cluster runs on one host (4 cores/4GB total across 2 workers), and 7.6M edges fits
comfortably in memory — real web-scale graphs (billions of edges) would need actual multi-node
placement, Parquet instead of a raw text scan, and a partition count tuned to data volume rather
than the fixed default this report argues down from. Just as importantly, BerkStan's in-degree
distribution never actually stressed the cluster — the salting mitigation in the tuning notes
above is argued from first principles, not exercised against a genuinely skewed key distribution,
so it's unverified against this codebase specifically.

## References
- Apache Spark, *Spark Standalone Mode* — https://spark.apache.org/docs/latest/spark-standalone.html
- Apache Spark, *Monitoring and Instrumentation* (Spark UI, History Server) — https://spark.apache.org/docs/latest/monitoring.html
- Apache Spark, *Configuration* (`spark.sql.shuffle.partitions` and other tuning knobs) — https://spark.apache.org/docs/latest/configuration.html
- Apache Spark, *PySpark DataFrame API Reference* — https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
- SNAP, *web-BerkStan dataset* — https://snap.stanford.edu/data/web-BerkStan.html
- Docker Hub, `apache/spark` official image — https://hub.docker.com/r/apache/spark

## Reproducing
```bash
cd task3-spark
docker compose up -d
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g --executor-cores 2 --total-executor-cores 4 \
  /opt/spark-apps/indegree_job.py
```
Master UI: http://localhost:8080 · History Server: http://localhost:18080
