# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "influxdb-client",
#     "python-dotenv",
# ]
# ///
"""Ingest the Fairbanks hourly climate CSV into InfluxDB, preserving original timestamps."""

import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

TASK_DIR = Path(__file__).resolve().parent.parent
load_dotenv(TASK_DIR / ".env")

CSV_PATH = TASK_DIR / "data" / "fairbanks_climate.csv"
INFLUX_URL = "http://localhost:8086"
INFLUX_ORG = os.environ["INFLUXDB_INIT_ORG"]
INFLUX_BUCKET = os.environ["INFLUXDB_INIT_BUCKET"]
INFLUX_TOKEN = os.environ["INFLUXDB_INIT_ADMIN_TOKEN"]

MEASUREMENT = "climate"
BATCH_SIZE = 5000


def read_points():
    with CSV_PATH.open(newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0] == "time":
                break
        else:
            raise RuntimeError(f"CSV header row not found in {CSV_PATH}")

        for row in reader:
            if not row:
                continue
            ts_str, temp_str, precip_str = row
            ts = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
            yield (
                Point(MEASUREMENT)
                .tag("station", "fairbanks")
                .field("temperature_c", float(temp_str))
                .field("precipitation_mm", float(precip_str))
                .time(ts, WritePrecision.S)
            )


def main():
    if not CSV_PATH.exists():
        sys.exit(f"CSV not found at {CSV_PATH}")

    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        batch, total = [], 0
        for point in read_points():
            batch.append(point)
            if len(batch) >= BATCH_SIZE:
                write_api.write(bucket=INFLUX_BUCKET, record=batch)
                total += len(batch)
                print(f"  wrote {total} points...")
                batch = []
        if batch:
            write_api.write(bucket=INFLUX_BUCKET, record=batch)
            total += len(batch)

    print(f"Done. Ingested {total} points into bucket '{INFLUX_BUCKET}'.")


if __name__ == "__main__":
    main()
