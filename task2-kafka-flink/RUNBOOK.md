# Task 2 Runbook — Kafka + Flink

Operational steps to bring up, run, verify, and tear down the Task 2 stack. See `README.md` for
the design writeup and rationale; this file is just the commands.

All commands assume `cd task2-kafka-flink` unless noted otherwise.

## 1. Start the stack

```bash
docker compose up -d
docker compose ps
```

Wait for `kafka` to report `(healthy)` before continuing (healthcheck polls every 10s).

First run (or after editing `flink-python/Dockerfile`) builds the custom Flink image — takes a
few minutes (installs Python3, a full JDK, gcc, `apache-flink`, downloads the Kafka connector
JAR). Subsequent starts reuse the cached image.

## 2. Create the topic

```bash
./scripts/create_topic.sh
```

Creates `traffic-telemetry` (3 partitions, RF 1). Safe to re-run (`--if-not-exists`). Also happens
automatically the first time `producer.py` runs, via its own `AdminClient` check — this script is
just the explicit, greppable version of that step.

Verify:
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic traffic-telemetry
```

## 3. Run the producer

```bash
cd scripts
uv run producer.py                                    # full 5000-row run, one msg/2s (~2.8h)
uv run producer.py --limit 450 --interval-seconds 0.02 # fast burst for testing/demo
```

Reads `data/camera_traffic_counts.csv` (committed to git — see README for why), sorts by
`read_date` ascending, publishes JSON keyed by `atd_device_id`. Stop anytime with Ctrl+C.

Verify messages landed:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic traffic-telemetry \
  --from-beginning --property print.key=true --max-messages 5 --timeout-ms 10000
```

Or visually, via Kafka UI — see step 5.

## 4. Submit the Flink job

```bash
docker exec jobmanager flink run -d -py /opt/flink/jobs/traffic_windowed_totals.py
```

`-d` = detached (runs on the cluster, doesn't block your terminal). `flink run` will print
`WARNING: Unknown module: jdk.compiler ...` a few times — harmless, ignore it. Note the printed
`JobID`.

**Recommended order for a clean demo:** produce a batch first (step 3), *then* submit the job —
it starts from `scan.startup.mode = earliest-offset`, so it reads the whole batch as one continuous
push instead of trickling in live. See the README's "DataStream API → Table API" note for why this
job is written as SQL rather than DataStream Python — the DataStream version silently never fired.

## 5. Verify it's working

Kafka UI: [http://localhost:8085](http://localhost:8085) — browse topics, partitions, offsets,
and consumer groups visually (`kafbat/kafka-ui`, connects to the broker automatically).

Flink dashboard: [http://localhost:8081](http://localhost:8081) — job should show `RUNNING`.

```bash
# via REST
curl -s http://localhost:8081/jobs/overview | python3 -m json.tool

# actual window output (print-connector sink writes to TaskManager stdout)
docker logs taskmanager | grep -E "\+I\["
```

Expect rows like:
```
2> +I[2024-07-08T12:30, 2024-07-08T12:45, 6653, 313]
```
Columns: `window_start, window_end, atd_device_id, window_total_volume`. `+I` = Flink changelog
"insert" marker (normal for append-only windowed aggregates). If you see
`numRecordsIn` growing on the window operator but no `+I[` rows after a minute or so, something's
wrong — don't assume it just needs more time (see Troubleshooting below).

## 6. Tear down / reset

Cancel the job (frees the task slot, doesn't stop the cluster):
```bash
docker exec jobmanager flink list                      # find the JobID if you don't have it
docker exec jobmanager flink cancel <JOB_ID>
```

Reset the topic to a clean slate (clears accumulated test messages + stale consumer-group
offsets — do this before a fresh demo run if the topic has old data mixed in):
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic traffic-telemetry
./scripts/create_topic.sh
```

Stop everything:
```bash
docker compose down          # keeps images, removes containers
docker compose down --rmi local   # also removes the built flink-python image
```

## Troubleshooting

**Job stuck at `numRecordsIn` growing, `+I[` never appears, even after minutes of real time.**
Don't assume it's just slow — we hit a real bug here (see README, "DataStream API → Table API"
note). If `jobs/traffic_windowed_totals.py` ever gets rewritten back into the DataStream Python
API (`.map()`/`.key_by()`/`.window()`/`.reduce()`), watermark propagation through PyFlink's Beam
Python-worker bridge is unreliable and windows may never fire. Stick to the Table API/SQL version.

**One Kafka partition has offset 0 forever.** Expected with only 7 sensor IDs hashed across 3
partitions — not every partition is guaranteed traffic. Handled by
`'scan.watermark.idle-timeout' = '20s'` in the job's source table DDL, which excludes idle
partitions from the watermark computation. Without it, Flink takes the *minimum* watermark across
partitions, so one empty partition stalls the whole job forever.

**`docker compose build` fails with apt `Hash Sum mismatch` on `ports.ubuntu.com`.** Transient
upstream ARM64 mirror issue, not a real problem with the Dockerfile — the build already retries
5x and uses HTTPS for that reason. If it still fails, just re-run `docker compose build`.

**`pip3 install apache-flink` fails on `Python.h: No such file` or JDK `include` folder missing.**
Already handled in `flink-python/Dockerfile` (`python3-dev` + a full `openjdk-11-jdk-headless`,
since the stock Flink image only ships a JRE). If you're modifying the Dockerfile, don't drop
either of those.

**Producer says topic doesn't exist / wrong partition count.** Shouldn't happen —
`producer.py`'s `ensure_topic()` auto-creates `traffic-telemetry` with the correct 3
partitions/RF 1 if missing (Kafka's `auto.create.topics.enable` default would otherwise silently
create it with 1 partition on first `produce()`). If you see 1 partition, something bypassed this
— check for any other producer/client that touched the topic first.
