#!/usr/bin/env bash
# Streams the first 5,000 non-comment edges from the SNAP cit-Patents dataset
# (stops early, avoids downloading the full ~85MB gzip) and writes them as a
# CSV with a header, ready for Neo4j's LOAD CSV in scripts/load_patents.cypher.
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p import

echo "source,target" > import/cit-patents-5000.csv
curl -s https://snap.stanford.edu/data/cit-Patents.txt.gz \
  | gunzip -c \
  | grep -v '^#' \
  | head -5000 \
  | tr '\t' ',' \
  >> import/cit-patents-5000.csv

echo "Wrote $(($(wc -l < import/cit-patents-5000.csv) - 1)) edges to import/cit-patents-5000.csv"
