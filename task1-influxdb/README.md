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

**Status:** Step 1.1 done (InfluxDB running in Docker). Step 1.2 done — `scripts/ingest.py` (uv,
`influxdb-client`) parses `data/fairbanks_climate.csv`, preserves original hourly UTC timestamps, and
writes to the `fairbanks_climate` bucket in batches of 5000. Verified: 87,672 points ingested,
queried back via Flux and confirmed values/timestamps match source CSV exactly. Next: Step 1.3
(Flux queries — hourly window aggregation, anomaly detection, downsampling task).

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
