# Task 1: Manual Run Path

All commands run from `task1-influxdb/`.

## 1. Start InfluxDB + Grafana

```bash
docker compose up -d
```

Wait ~5s, then confirm InfluxDB is healthy:

```bash
curl -s http://localhost:8086/health
```

## 2. Ingest the climate data

Parses `data/fairbanks_climate.csv` (Open-Meteo hourly data, 2015-01-01 through today) and writes
each row to InfluxDB with its original historical timestamp:

```bash
uv run scripts/ingest.py
```

Verify the point count landed correctly:

```bash
source .env
curl -s "http://localhost:8086/api/v2/query?org=$INFLUXDB_INIT_ORG" \
  -H "Authorization: Token $INFLUXDB_INIT_ADMIN_TOKEN" \
  -H "Accept: application/csv" -H "Content-type: application/vnd.flux" \
  -d 'from(bucket:"fairbanks_climate") |> range(start: 2015-01-01T00:00:00Z, stop: now())
        |> filter(fn: (r) => r._field == "temperature_c") |> count()'
```

## 3. Run the Flux queries

**Hourly window aggregation** and **anomaly detection** are plain read-only queries — paste
either file's contents into the InfluxDB UI's Data Explorer (Script Editor) at
`http://localhost:8086`, or run via curl:

```bash
curl -s "http://localhost:8086/api/v2/query?org=$INFLUXDB_INIT_ORG" \
  -H "Authorization: Token $INFLUXDB_INIT_ADMIN_TOKEN" \
  -H "Accept: application/csv" -H "Content-type: application/vnd.flux" \
  -d "$(cat queries/1_hourly_window.flux)"

curl -s "http://localhost:8086/api/v2/query?org=$INFLUXDB_INIT_ORG" \
  -H "Authorization: Token $INFLUXDB_INIT_ADMIN_TOKEN" \
  -H "Accept: application/csv" -H "Content-type: application/vnd.flux" \
  -d "$(cat queries/2_anomaly_detection.flux)"
```

**Downsampling task** needs one-time setup — create the 30-day-retention bucket and register the
recurring task (only needs doing once; skip if already done):

```bash
docker exec task1-influxdb influx bucket create --name fairbanks_climate_downsampled \
  --org "$INFLUXDB_INIT_ORG" --token "$INFLUXDB_INIT_ADMIN_TOKEN" --retention 30d

FLUX=$(cat queries/3_downsampling_task.flux)
curl -s -X POST "http://localhost:8086/api/v2/tasks" \
  -H "Authorization: Token $INFLUXDB_INIT_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d "$(python3 -c "import json,sys; print(json.dumps({'org': '$INFLUXDB_INIT_ORG', 'flux': sys.stdin.read()}))" <<< "$FLUX")"
```

Since the source data is kept refreshed through today (see README), the task naturally has real
recent data to summarize on each daily run. To backfill immediately instead of waiting for the
next scheduled run, execute the same aggregation logic as a one-off write (see the comment block
in `queries/3_downsampling_task.flux` for the exact query).

## 4. View it in Grafana (optional)

```
http://localhost:3000
```

Login: `$GRAFANA_ADMIN_USER` / `$GRAFANA_ADMIN_PASSWORD` (in `.env`). Dashboard
"Fairbanks Climate — InfluxDB" is auto-provisioned on startup — no manual setup needed.

## Teardown

```bash
docker compose down
```

This only stops the containers — InfluxDB's data persists in the bind-mounted `./influxdb-data/`
and Grafana's in `./grafana-data/`, both still there on the next `docker compose up -d`. The
bucket/task registration from Step 3 is stored inside InfluxDB itself, so it survives too — no
need to re-run bucket creation or task registration after a normal restart.

Only wipe the data if you want to prove the pipeline reproduces cleanly from empty:

```bash
docker compose down
rm -rf influxdb-data/* grafana-data/*
docker compose up -d
# then repeat steps 2-3
```
