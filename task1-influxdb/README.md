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

**Status:** Step 1.1 done (InfluxDB running in Docker). Dataset acquired, Step 1.2 (ingestion script) next.
