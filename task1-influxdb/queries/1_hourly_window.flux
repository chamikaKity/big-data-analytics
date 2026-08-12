// Step 1.3 (1/3): Window Aggregation — sliding hourly average of temperature
// across the full observation span (2015-01-01 through today).
//
// Run via InfluxDB UI (Data Explorer > Script Editor, paste + Submit) or CLI:
//   docker exec -it task1-influxdb influx query --org "$INFLUXDB_INIT_ORG" --token "$INFLUXDB_INIT_ADMIN_TOKEN" -f /path/to/this/file
// or via the HTTP API (see README for the curl example).

from(bucket: "fairbanks_climate")
  |> range(start: 2015-01-01T00:00:00Z, stop: now())
  |> filter(fn: (r) => r._measurement == "climate" and r._field == "temperature_c")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> yield(name: "hourly_mean_temp")
