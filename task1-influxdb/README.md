# Task 1: Distributed Time-Series Data Management using InfluxDB

**Objective:** Provision InfluxDB v2.x in Docker, ingest the Fairbanks climate CSV with original
historical timestamps, and run Flux queries for hourly aggregation, anomaly detection, and
downsampling with a 30-day retention policy.

**Dataset:** The link in the assignment brief (`snap.uaf.edu/tools/community-charts/data/fairbanks_climate.csv`)
no longer serves raw data — it now redirects to a single-page app, and the underlying SNAP database
turned out to be decadal climate *projections* (20-26 summary rows, no per-timestamp readings), which
can't support hourly aggregation/anomaly/downsampling queries. Substituted with 10 years (2015-2024)
of real hourly observations for Fairbanks (64.8378, -147.716) from the
[Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) (free, no
API key). Saved at `data/fairbanks_climate.csv` (~87.7k rows, gitignored).

Exact request used to fetch the data:
```
https://archive-api.open-meteo.com/v1/archive?latitude=64.8378&longitude=-147.716&start_date=2015-01-01&end_date=2024-12-31&hourly=temperature_2m,precipitation&timezone=UTC&format=csv
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
from the dataset mean. Computed over the full 2015-2024 span: mean ≈ -0.36°C, stddev ≈ 14.42,
thresholds ≈ [-29.20°C, 28.48°C]. Result: **2,253 of 87,672 readings (~2.6%)** flagged. Most extreme:
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

**Important finding, worth knowing for the viva:** InfluxDB retention is relative to the *current*
clock, not to when data is written. A 30-day-retention bucket only keeps points timestamped within
the last 30 days of *now* — writing our 2015-2024 backfill into it gets silently dropped almost
immediately, since every point is over a decade "expired" relative to today. Verified directly: a
test point written with today's timestamp persisted correctly; the full historical backfill did not.
This also means the live scheduled task (which looks back 1 day from *now* in the source bucket each
run) finds nothing to summarize, since the historical source data doesn't extend to the present.

This is expected, correct InfluxDB behavior for a bucket designed to hold rolling recent summaries,
not a bug — it just structurally conflicts with backfilling old historical data specifically. So the
demonstration here is split into what each part actually proves:
- Task + 30-day-retention bucket are correctly configured (shown above).
- Retention enforcement is verified working (recent-timestamp test point persisted; decade-old
  backfill did not).
- The aggregation logic itself is verified correct via a read-only query (no `to()` write, so not
  subject to retention), e.g. for Dec 2024:
```
2024-12-02  ->  -25.36°C
2024-12-03  ->  -24.67°C
2024-12-04  ->  -19.18°C
2024-12-05  ->   -7.64°C
...
```
Each value is the mean of 24 hourly readings for that day — correct and consistent with Fairbanks'
real seasonal pattern.
