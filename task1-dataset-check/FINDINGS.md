# Dataset Suitability Check

Checked the two Kaggle datasets suggested as replacements for Task 1's broken SNAP link. Both
were actually downloaded and ingested into an isolated InfluxDB instance to verify real-world
suitability, not just reviewed on paper.

## What works and what doesn't

- **1.1 Environment** — no dataset dependency, unaffected either way.
- **1.2 Ingestion (Dataset A — Szeged)** — works. 96,429 / 96,453 rows ingested (24-row gap =
  confirmed duplicate timestamps already in the source CSV, not an ingestion error).
- **1.3, query 1 — hourly window aggregation (Dataset B — rohitgrewal)** — works.
- **1.3, query 2 — anomaly detection (Dataset B)** — works.
- **1.3, query 3 — downsampling task, 30-day retention (Dataset B)** — **breaks.** The task and
  the retention-bound bucket can still be configured correctly, but the bucket can't hold real
  persisted data, since Dataset B's newest data (2012-12-31) is ~14 years too old to ever fall
  inside a "last 30 days from now" window.
- **Using two datasets means repeating 1.2** — Flux queries only run on data already in
  InfluxDB, so Dataset B needs its own ingestion pass too (own columns, own date format, own
  load) before any 1.3 query can run on it. That's Step 1.2's work done twice, with two schemas
  to document instead of one.

## Why the 30-day bucket breaks

| Dataset | Time Range | Latest Date | Gap to Today (2026) |
|---|---|---|---|
| A — Szeged Weather | 2006-04-01 to 2016-09-09 | 2016-09-09 | ~10 years too old |
| B — rohitgrewal Weather Data | 2012-01-01 to 2012-12-31 | 2012-12-31 | ~14 years too old |

A 30-day-retention bucket only keeps points timestamped within the last 30 days of *right now*.
Both datasets are static, one-time downloads — neither has data anywhere near today, so neither
can populate this bucket. This isn't about which dataset we pick; it happens with any fixed file.
Using just one dataset for all of Task 1 (simpler — one ingestion script, one schema, one story)
wouldn't fix it either: Dataset A is just as frozen as Dataset B.

## Dataset details

**A — [Szeged Weather](https://www.kaggle.com/datasets/budincsevity/szeged-weather)** (2006-2016,
hourly, 96,453 rows) — `time, summary, precipType, temperature, apparentTemperature, humidity,
windSpeed, windBearing, visibility, loudCover, pressure`. License: CC BY-NC-SA 4.0.

**B — [rohitgrewal Weather Data](https://www.kaggle.com/datasets/rohitgrewal/weather-data)**
(2012 only, hourly, 8,784 rows) — `Date/Time, Temp_C, Dew Point Temp_C, Rel Hum_%,
Wind Speed_km/h, Visibility_km, Press_kPa, Weather`. License: ODbL.

## Suggestion: keep the dataset already used in Task 1

**Dataset:** [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
```
https://archive-api.open-meteo.com/v1/archive?latitude=64.8378&longitude=-147.716&start_date=2015-01-01&end_date=2026-08-12&hourly=temperature_2m,precipitation&timezone=UTC&format=csv
```

- **Only option that fully satisfies the 30-day retention requirement** — it's a live API,
  re-queryable through today's date, unlike either static Kaggle file.
- **Already built, verified, and documented** — ingestion (101,808 points), all three Flux
  queries (retention task demonstrated with real persisted data), Grafana dashboard, written
  report with screenshots.
- **Switching now = redoing complete work for a weaker result** — a Kaggle dataset would still
  need the retention conflict caveated or worked around.
- **Legitimate, citable source** — ERA5 reanalysis (ECMWF/Copernicus), CC BY 4.0, already cited
  in the Task 1 report.
