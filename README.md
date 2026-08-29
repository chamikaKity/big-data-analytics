# Big Data Analytics — Coursework

Postgraduate coursework for the Big Data Analytics Technologies module. All practical components
run in Docker containers.

| Folder | Task |
|---|---|
| [`task1-influxdb`](task1-influxdb) | Time-series data management with InfluxDB |
| [`task2-kafka-flink`](task2-kafka-flink) | Real-time stream processing with Kafka + Flink |
| [`task3-spark`](task3-spark) | Scalable graph analytics with Apache Spark |
| [`task4-neo4j`](task4-neo4j) | Graph database engineering with Neo4j |
| [`task5-governance-report`](task5-governance-report) | Data governance & future trends essay |

Together the four practical tasks trace one data pipeline's lifecycle end to end: Task 1 stores
and queries a single sensor's readings over time (InfluxDB); Task 2 moves the same kind of data
from static storage into a live, continuously-arriving stream, windowed and aggregated in real
time (Kafka + Flink); Task 3 steps back from any one stream to batch-process a dataset at genuine
scale, distributing the computation itself across a cluster (Spark); and Task 4 takes that
computed structure and makes it persistently queryable by relationship rather than by row (Neo4j).
Read in order, they cover storage → streaming → distributed batch compute → graph modeling — the
main paradigms a real big-data system has to combine, not four unrelated exercises.

Each folder contains its own README with the task objective and dataset link.
