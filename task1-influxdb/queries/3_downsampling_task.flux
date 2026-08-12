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
// mean per field. Since the source data is historical (2015-2024, not live), a
// one-time manual backfill (same aggregation logic, full date range) was also run
// once so the downsampled bucket has visible data immediately — see README.

option task = {name: "downsample-daily-climate", every: 1d}

from(bucket: "fairbanks_climate")
  |> range(start: -task.every)
  |> filter(fn: (r) => r._measurement == "climate")
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
  |> to(bucket: "fairbanks_climate_downsampled", org: "cu-bigdata")
