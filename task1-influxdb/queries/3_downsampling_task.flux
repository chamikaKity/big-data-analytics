// Step 1.3 (3/3): Downsampling Task — continuously summarizes hourly climate
// data into a daily average, stored in a second bucket governed by an
// explicit 30-day retention policy.
//
// Target bucket created via:
//   docker exec task1-influxdb influx bucket create --name fairbanks_climate_downsampled \
//     --org "$INFLUXDB_INIT_ORG" --token "$INFLUXDB_INIT_ADMIN_TOKEN" --retention 30d
//
// This task is registered with InfluxDB (via the Tasks API) and runs automatically
// once every day, aggregating the previous day's hourly readings into one daily
// mean per field.
//
// Note: a bucket with 30-day retention only ever keeps data timestamped within the
// last 30 days of *now* — writing historical (e.g. 2015-2024) backfill into it gets
// silently dropped almost immediately, since it's expired relative to today. The
// source dataset is therefore kept refreshed through the current date (see README),
// so genuinely recent data exists for this task/backfill to populate — verified: the
// downsampled bucket holds 31 real daily points and correctly ages out anything older.

// One-time backfill (source dataset now spans 2015-01-01 through today), same
// aggregation logic as the task itself, run manually once so the bucket has
// immediately-visible data rather than waiting a full day for the first scheduled run:
//   from(bucket: "fairbanks_climate")
//     |> range(start: 2015-01-01T00:00:00Z, stop: now())
//     |> filter(fn: (r) => r._measurement == "climate")
//     |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
//     |> to(bucket: "fairbanks_climate_downsampled", org: "cu-bigdata")

option task = {name: "downsample-daily-climate", every: 1d}

from(bucket: "fairbanks_climate")
  |> range(start: -task.every)
  |> filter(fn: (r) => r._measurement == "climate")
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
  |> to(bucket: "fairbanks_climate_downsampled", org: "cu-bigdata")
