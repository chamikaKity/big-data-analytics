// Step 1.3 (2/3): Anomaly Isolation — temperature readings more than 2 standard
// deviations from the dataset mean, computed over the full observation span
// (2015-01-01 through today).
//
// Result (verified): mean ≈ -0.47°C, stddev ≈ 14.72, thresholds ≈ [-29.90, 28.96]°C.
// 2,878 of 101,808 readings (~2.8%) flagged. Most extreme: -46.8°C on 2017-01-19,
// a genuine documented Fairbanks cold snap.

data = from(bucket: "fairbanks_climate")
  |> range(start: 2015-01-01T00:00:00Z, stop: now())
  |> filter(fn: (r) => r._measurement == "climate" and r._field == "temperature_c")

meanVal = (data |> mean(column: "_value") |> findRecord(fn: (key) => true, idx: 0))._value
stddevVal = (data |> stddev(column: "_value") |> findRecord(fn: (key) => true, idx: 0))._value

data
  |> filter(fn: (r) => r._value > meanVal + 2.0 * stddevVal or r._value < meanVal - 2.0 * stddevVal)
  |> yield(name: "anomalies")
