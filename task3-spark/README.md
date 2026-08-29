# Task 3: Scalable Data Analytics with Apache Spark

**Objective:** Run a standalone Spark cluster in Docker (1 master, 2 workers capped at 2 cores/2GB
each), and use PySpark to parse the SNAP web-BerkStan graph, compute in-degree distributions,
and identify the top 50 destination nodes with caching/broadcast optimizations. Capture DAG,
stage, and shuffle metrics from the Spark UI.

**Dataset:** https://snap.stanford.edu/data/web-BerkStan.html — not committed (gitignored, ~105MB
uncompressed). Fetch it into `data/` before running the cluster:
```
curl -sSL -o data/web-BerkStan.txt.gz https://snap.stanford.edu/data/web-BerkStan.txt.gz
gunzip -k data/web-BerkStan.txt.gz
```

**Status:** 3.1–3.3 done — cluster up, in-degree job written and run, results in
`Task-3-Report.md` (screenshots pending)
