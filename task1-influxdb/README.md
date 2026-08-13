# Task 1: Distributed Time-Series Data Management using InfluxDB

**Objective:** Provision InfluxDB v2.x in Docker, ingest the Fairbanks climate CSV with original
historical timestamps, and run Flux queries for hourly aggregation, anomaly detection, and
downsampling with a 30-day retention policy.

**Dataset:** The link in the assignment brief (`snap.uaf.edu/tools/community-charts/data/fairbanks_climate.csv`)
no longer serves raw data — it now redirects to a single-page app, and the underlying SNAP database
turned out to be decadal climate *projections* (20-26 summary rows, no per-timestamp readings), which
can't support hourly aggregation/anomaly/downsampling queries. Substituted with real hourly
observations for Fairbanks (64.8378, -147.716) from the
[Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) (free, no
API key), from **2015-01-01 through the current date** (kept refreshed through today so the dataset
has genuinely recent data too — see Step 1.3 downsampling notes below for why that matters). Saved at
`data/fairbanks_climate.csv` (~101.8k rows as of 2026-08-12, gitignored).

Request used to fetch the data (re-run with an updated `end_date` to keep it current):
```
https://archive-api.open-meteo.com/v1/archive?latitude=64.8378&longitude=-147.716&start_date=2015-01-01&end_date=2026-08-12&hourly=temperature_2m,precipitation&timezone=UTC&format=csv
```

Underlying source: ERA5 reanalysis (ECMWF / Copernicus Climate Change Service), licensed CC BY 4.0.
Citation: Zippenfenig, P. (2023). *Open-Meteo.com Weather API* [Data set]. Open-Meteo.
https://doi.org/10.5281/zenodo.7970649

**Status:** Steps 1.1, 1.2, 1.3 all done and verified. See `queries/` for the three Flux
queries and details below.

### Line Protocol

The ingest script builds each point with the official client's `Point()` builder (schema: measurement
`climate`, tag `station=fairbanks`, fields `temperature_c`/`precipitation_mm`, second-precision
timestamp), which the client serializes into InfluxDB Line Protocol before writing. For CSV row
`2015-01-01T00:00,-1.8,0.00`, the resulting Line Protocol is:

```
climate,station=fairbanks temperature_c=-1.8,precipitation_mm=0.00 1420070400
```

`1420070400` is the Unix epoch (seconds) for `2015-01-01T00:00:00Z` — the record's own historical
timestamp, not the time the script was run.

### Step 1.3 — Flux queries

**1. Window aggregation** (`queries/1_hourly_window.flux`): sliding hourly mean of `temperature_c`
via `aggregateWindow(every: 1h, fn: mean)`. Since the source is already hourly-resolution, output
values equal the raw readings — the transformation is correct, just not visually dramatic at this
granularity (verified against raw data).

**2. Anomaly isolation** (`queries/2_anomaly_detection.flux`): flags readings >2 standard deviations
from the dataset mean. Computed over the full 2015-2026 span: mean ≈ -0.47°C, stddev ≈ 14.72,
thresholds ≈ [-29.90°C, 28.96°C]. Result: **2,878 of 101,808 readings (~2.8%)** flagged. Most extreme:
**-46.8°C on 2017-01-19** — a real, documented Fairbanks cold snap, confirming the anomalies are
genuine extremes, not noise.

**3. Downsampling task** (`queries/3_downsampling_task.flux`): a recurring InfluxDB Task
(`downsample-daily-climate`, runs every `1d`) that aggregates hourly data into daily means, written
to a second bucket `fairbanks_climate_downsampled` created with an explicit 30-day retention policy:
```bash
docker exec task1-influxdb influx bucket create --name fairbanks_climate_downsampled \
  --org "$INFLUXDB_INIT_ORG" --token "$INFLUXDB_INIT_ADMIN_TOKEN" --retention 30d
```
Task registered via the Tasks API (`POST /api/v2/tasks`) — id `11296bf289d2a000`, status `active`.

**Key finding, worth knowing for the viva:** InfluxDB retention is relative to the *current* clock,
not to when data is written. A 30-day-retention bucket only keeps points timestamped within the last
30 days of *now* — writing old historical backfill into it gets silently dropped almost immediately,
since it's already "expired" relative to today. Verified directly: a test point written with today's
timestamp persisted correctly; a backfill using only the original 2015-2024 range did not.

This is expected, correct InfluxDB behavior for a bucket designed to hold rolling recent summaries —
not a bug, but it does mean a purely historical (long-past) dataset can never populate a short-retention
bucket. **Resolution:** the source dataset is kept refreshed through the current date (see Dataset
section above), so it always contains genuinely recent data. Re-running the same daily-aggregation
logic (either the registered task or the equivalent one-off backfill query in
`3_downsampling_task.flux`) against this updated source data means real, current daily averages
correctly persist. Verified: `fairbanks_climate_downsampled` holds exactly the last 31 real days
(everything older correctly aged out by the retention policy), e.g.:
```
2026-07-14  ->  16.32°C
2026-07-15  ->  14.51°C
2026-07-16  ->  14.72°C
...
2026-08-11  ->  17.38°C
2026-08-12  ->  18.14°C
```
Each value is the mean of 24 hourly readings for that day, correctly reflecting Fairbanks' real
mid-summer temperature range.

### Grafana dashboard (bonus, beyond the required steps)

A Grafana instance is included in `docker-compose.yml`, fully provisioned via config files (no manual
UI clicking) — consistent with the Docker-only constraint:

- `grafana/provisioning/datasources/influxdb.yml` — auto-connects Grafana to InfluxDB (Flux query
  language, org/token/bucket pulled from env vars via Grafana's `$__env{...}` provisioning syntax).
- `grafana/provisioning/dashboards/dashboard.yml` — tells Grafana to auto-load dashboard JSON files.
- `grafana/dashboards/fairbanks-climate.json` — the dashboard itself, 5 panels:
  1. Hourly temperature (full history, `fairbanks_climate`)
  2. Hourly precipitation (full history)
  3. Downsampled daily average temperature (last 30 days, `fairbanks_climate_downsampled`)
  4. Anomaly count in the current view (>2 stddev, live-recomputed)
  5. Latest temperature reading (stat panel)

Access at **http://localhost:3000** (credentials in `.env` — `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD`).
Verified end-to-end: datasource health check confirms connectivity ("datasource is working, 4 buckets
found"), and a direct query through Grafana's API (`/api/ds/query`) against the downsampled-temperature
panel returned the same 31 real data points as the direct InfluxDB verification above.
