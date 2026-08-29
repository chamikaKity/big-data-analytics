# Task 3 Runbook — Spark

Operational steps to bring up, run, verify, and tear down the Task 3 stack. See `README.md` and
`Task-3-Report.md` for the design writeup and results; this file is just the commands.

All commands assume `cd task3-spark` unless noted otherwise.

## 1. Fetch the dataset

Not committed to git (gitignored, ~105MB uncompressed — see README for why):

```bash
curl -sSL -o data/web-BerkStan.txt.gz https://snap.stanford.edu/data/web-BerkStan.txt.gz
gunzip -k data/web-BerkStan.txt.gz
```

## 2. Start the cluster

```bash
docker compose up -d
docker compose ps
```

Brings up `spark-master` (UI :8080, RPC :7077), `spark-worker-1` / `spark-worker-2` (2 cores/2GB
each), and `spark-history` (History Server UI :18080).

Verify both workers registered:

```bash
curl -s http://localhost:8080/json/ | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('alive workers:', d['aliveworkers'])
"
```

Expect `alive workers: 2`. If it prints `0`, see Troubleshooting below.

## 3. Run the in-degree job

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  --executor-cores 2 \
  --total-executor-cores 4 \
  /opt/spark-apps/indegree_job.py
```

Parses `data/web-BerkStan.txt`, computes in-degree per destination vertex, prints the top 50, and
writes them to `data/output/top50_indegree/` (CSV). Takes ~12-15s end to end.

## 4. Verify the results

**Master UI** — [http://localhost:8080](http://localhost:8080): confirm both workers `ALIVE` and
the job listed under Completed Applications with state `FINISHED`.

**History Server** — [http://localhost:18080](http://localhost:18080): click into the
`BerkStan-InDegree` application for the full Jobs/Stages/DAG/Executors breakdown (this is what
`Task-3-Report.md`'s screenshots are captured from — event logs persist here even after the
driver process exits, so there's no rush to catch it live).

```bash
# via REST
curl -s http://localhost:18080/api/v1/applications | python3 -m json.tool
```

**Output file:**
```bash
cat data/output/top50_indegree/part-*.csv
```

## 5. Tear down / reset

```bash
docker compose down          # keeps images, removes containers
```

Data (`data/`), conf (`conf/spark-defaults.conf`), and event logs (`data/spark-events/`) are all
bind-mounted, so they survive a `down`/`up` cycle — no need to re-fetch the dataset or lose prior
run history.

## Troubleshooting

**Master UI shows `Alive Workers: 0` after the stack has been up for a long time (hours/overnight),
even though `docker compose ps` shows all containers still `Up`.** Real, repeatable issue on this
setup — the worker→master TCP connection silently drops after the host machine sleeps or the
Docker network hiccups, and the worker JVM doesn't reconnect on its own (container keeps running,
just disconnected). Fix:
```bash
docker compose restart spark-worker-1 spark-worker-2
```
Confirm with the `alive workers` check in step 2 before submitting a job — otherwise the app just
sits in `WAITING` state indefinitely (no executors to allocate).

**`spark-master` container exits immediately with `UnknownHostException: spark-master: Temporary
failure in name resolution`.** A startup race — `docker-compose.yml` sets `restart: on-failure` on
all three cluster services specifically to self-heal this; Spark's `Master`/`Worker` classes call
`Utils.localHostName()` before parsing the `--host` flag, and if the container's own `/etc/hosts`
entry isn't populated yet at that instant, it throws instead of retrying. If it doesn't recover
within a few seconds, force it: `docker compose up -d --force-recreate spark-master`.

**Bitnami Spark image tags (`bitnami/spark:3.5` etc.) fail with `manifest not found`.** Bitnami
moved versioned tags behind a paywall in 2025; free tags now only exist under `bitnamilegacy/`.
This project uses the official `apache/spark` image instead — already reflected in
`docker-compose.yml`, just noting it in case you're tempted to "fix" it back to `bitnami/spark`.
