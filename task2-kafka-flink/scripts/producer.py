"""Streams Austin Camera Traffic Counts CSV rows into the traffic-telemetry Kafka topic.

Rows are sorted by read_date ascending and published one every INTERVAL_SECONDS,
so downstream Flink event-time watermarks (2.3) advance monotonically.
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "camera_traffic_counts.csv"


def build_payload(row: dict) -> dict:
    return {
        "record_id": row["record_id"],
        "atd_device_id": row["atd_device_id"],
        "read_date": row["read_date"],
        "intersection_name": row["intersection_name"],
        "direction": row["direction"],
        "movement": row["movement"],
        "heavy_vehicle": row["heavy_vehicle"].lower() == "true",
        "volume": int(row["volume"]),
        "speed_average": float(row["speed_average"]) if row["speed_average"] else None,
        "bin_duration": int(row["bin_duration"]),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="traffic-telemetry")
    parser.add_argument("--csv-path", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=None, help="stop after N messages (default: all rows)")
    args = parser.parse_args()

    with args.csv_path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    rows.sort(key=lambda r: r["read_date"])
    if args.limit:
        rows = rows[: args.limit]

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    print(f"streaming {len(rows)} rows to '{args.topic}' every {args.interval_seconds}s")
    for i, row in enumerate(rows, start=1):
        payload = build_payload(row)
        producer.produce(
            args.topic,
            key=payload["atd_device_id"],
            value=json.dumps(payload),
            callback=delivery_report,
        )
        producer.poll(0)
        if i % 50 == 0 or i == len(rows):
            print(f"  sent {i}/{len(rows)} (last read_date={payload['read_date']})")
        time.sleep(args.interval_seconds)

    producer.flush()
    print("done")


if __name__ == "__main__":
    main()
