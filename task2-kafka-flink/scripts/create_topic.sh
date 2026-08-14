#!/usr/bin/env bash
# Creates the traffic-telemetry topic (3 partitions, replication factor 1)
# used by producer.py and the Flink job. Safe to re-run: kafka-topics.sh
# fails harmlessly if the topic already exists.
set -uo pipefail

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic traffic-telemetry --partitions 3 --replication-factor 1 \
  --if-not-exists

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic traffic-telemetry
