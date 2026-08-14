# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "influxdb-client",
#     "python-dotenv",
# ]
# ///
"""Quick ingestion test for both lecturer-suggested datasets, to verify they actually
load into InfluxDB cleanly with correct historical timestamps."""

import csv
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

TASK_DIR = Path(__file__).resolve().parent.parent
load_dotenv(TASK_DIR / ".env")

INFLUX_URL = "http://localhost:8087"
INFLUX_ORG = os.environ["INFLUXDB_INIT_ORG"]
INFLUX_BUCKET = os.environ["INFLUXDB_INIT_BUCKET"]
INFLUX_TOKEN = os.environ["INFLUXDB_INIT_ADMIN_TOKEN"]
BATCH_SIZE = 5000


def write_points(write_api, points):
    batch = []
    total = 0
    for p in points:
        batch.append(p)
        if len(batch) >= BATCH_SIZE:
            write_api.write(bucket=INFLUX_BUCKET, record=batch)
            total += len(batch)
            batch = []
    if batch:
        write_api.write(bucket=INFLUX_BUCKET, record=batch)
        total += len(batch)
    return total


def szeged_points():
    path = TASK_DIR / "data" / "weatherHistory.csv"
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # "2006-04-01 00:00:00.000 +0200"
            ts = datetime.strptime(row["Formatted Date"], "%Y-%m-%d %H:%M:%S.%f %z")
            yield (
                Point("szeged_weather")
                .tag("precip_type", row["Precip Type"] or "none")
                .field("temperature_c", float(row["Temperature (C)"]))
                .field("humidity", float(row["Humidity"]))
                .field("wind_speed_kmh", float(row["Wind Speed (km/h)"]))
                .field("pressure_mb", float(row["Pressure (millibars)"]))
                .time(ts, WritePrecision.S)
            )


def rohitgrewal_points():
    path = TASK_DIR / "data" / "Project 1 - Weather Dataset.csv"
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # "1/1/2012 0:00"
            ts = datetime.strptime(row["Date/Time"], "%m/%d/%Y %H:%M")
            yield (
                Point("rohitgrewal_weather")
                .tag("weather", row["Weather"] or "none")
                .field("temp_c", float(row["Temp_C"]))
                .field("humidity_pct", float(row["Rel Hum_%"]))
                .field("wind_speed_kmh", float(row["Wind Speed_km/h"]))
                .field("pressure_kpa", float(row["Press_kPa"]))
                .time(ts, WritePrecision.S)
            )


def main():
    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        n1 = write_points(write_api, szeged_points())
        print(f"Szeged: ingested {n1} points into measurement 'szeged_weather'")

        n2 = write_points(write_api, rohitgrewal_points())
        print(f"rohitgrewal: ingested {n2} points into measurement 'rohitgrewal_weather'")


if __name__ == "__main__":
    main()
