# Dataset Suitability Check — for lecturer review

Testing two Kaggle datasets suggested as replacements for Task 1's original (broken) SNAP link.
Both were actually downloaded and ingested into an isolated InfluxDB instance (not the real
Task 1 setup) to verify real-world suitability, not just theoretical review.

## Dataset A — [Szeged Weather 2006-2016](https://www.kaggle.com/datasets/budincsevity/szeged-weather) (suggested for Steps 1.1/1.2)

- Genuine hourly time series, one row per hour, **2006-04-01 to 2016-09-09** (~10 years)
- Columns: `time, summary, precipType, temperature, apparentTemperature, humidity, windSpeed,
  windBearing, visibility, loudCover, pressure`
- License: CC BY-NC-SA 4.0 (non-commercial — fine for coursework, requires citation)
- **Ingestion test**: 96,453 CSV rows → 96,429 points stored in InfluxDB. The 24-point gap is
  confirmed as 24 genuine duplicate timestamps in the source CSV (verified directly), not an
  ingestion error — InfluxDB correctly deduplicates identical measurement+tag+timestamp writes.
- **Verdict: Suitable.** Richer schema than the original substitute dataset (temperature,
  humidity, wind, pressure all in one place — good material for tag/field design discussion).

## Dataset B — [Weather Data (rohitgrewal)](https://www.kaggle.com/datasets/rohitgrewal/weather-data) (suggested for Step 1.3)

- Genuine hourly time series, one row per hour, but only **one fixed year: 2012-01-01 to
  2012-12-31** (8,784 rows)
- Columns: `Date/Time, Temp_C, Dew Point Temp_C, Rel Hum_%, Wind Speed_km/h, Visibility_km,
  Press_kPa, Weather`
- License: Open Database License (ODbL)
- **Ingestion test**: 8,784 CSV rows → 8,784 points stored, exact match, no issues.
- **Verdict: Works for windowing and anomaly detection (Step 1.3, parts 1-2) with no problems.**
  For the downsampling task with 30-day retention (Step 1.3, part 3), there's a structural
  conflict: InfluxDB retention is relative to the *current* clock, not the data's own age — a
  30-day-retention bucket only keeps points timestamped within the last 30 days of *now*. Since
  this dataset is a fixed 2012 snapshot with no way to fetch newer data (unlike the original
  Open-Meteo substitute, which is a live API we could re-query through today), there is no way
  to get genuinely current data into that bucket from this dataset alone.
  - We hit this exact issue with the original substitute dataset too, and resolved it by
    re-fetching through today's date (a live API). That fix isn't available here.
  - Options if this dataset is used for 1.3: (a) accept a "task + bucket configured correctly,
    but demonstrated via a read-only query rather than a persisted downsampled bucket" writeup,
    or (b) time-shift a slice of 2012 data to look recent for demo purposes only, clearly
    documented as such.

## Note: switching the Step 1.3 dataset means redoing ingestion too
Flux queries can only run against data already sitting in an InfluxDB bucket. If Dataset B gets
swapped for something else to work around the retention conflict, that replacement also needs a
full ingestion pass first (parse CSV, map columns to InfluxDB points, preserve original
timestamps, write to a bucket) — functionally the same work as Step 1.2, just applied to a
different file. There's no way to skip straight to writing 1.3 queries against an unloaded
dataset.

## Should we just use one dataset for all of Task 1?
Using a single dataset throughout (1.1, 1.2, and 1.3) would be simpler and more coherent than
juggling two — one ingestion pipeline, one schema, one consistent story for the viva, instead of
double the ingestion work and two sets of column-mapping logic. If going that route, **Dataset A
(Szeged) is the better single choice**: 10 years of data vs. Dataset B's 1 year, and a richer
schema (temperature, humidity, wind, pressure vs. just temperature/humidity/wind/pressure at
lower variety) gives more to work with for the anomaly and windowing queries.

**Important caveat:** consolidating to one dataset does **not** by itself fix the retention/
downsampling conflict above. Dataset A is *also* a fixed historical snapshot (2006-2016) with no
live API behind it — the same problem that affects Dataset B. The only reason the original
Open-Meteo substitute dataset could solve this was that it's backed by a live, re-queryable API
we could pull fresh data from through today; neither Kaggle dataset offers that. So the retention
question is independent of which dataset(s) get used, and is still worth raising with the
lecturer directly regardless of the final dataset decision.

## Recommendation
Both datasets are technically ingestible and verified working. Using Dataset A alone for all of
Task 1 would be the cleanest setup if the lecturer is open to it. If both datasets stay as
assigned (A for 1.1/1.2, B for 1.3), Dataset B works fine for the windowing and anomaly queries
but reproduces the same retention/historical-data conflict already documented in the current
submission. Either way, worth asking directly whether a read-only/documented demonstration is
acceptable for the downsampling part, since no available dataset resolves it outright.
