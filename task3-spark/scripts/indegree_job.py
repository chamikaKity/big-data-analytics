"""
Task 3.2 — PySpark in-degree analysis of the SNAP web-BerkStan graph.

Lazily parses the raw edge list, strips '#' comment headers, computes the
in-degree of every destination vertex, and reports the top 50 nodes.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_PATH = "/opt/spark-data/web-BerkStan.txt"
OUTPUT_PATH = "/opt/spark-data/output/top50_indegree"


def main() -> None:
    spark = SparkSession.builder.appName("BerkStan-InDegree").getOrCreate()

    # spark.read.text is lazy — nothing executes until an action below.
    lines = spark.read.text(DATA_PATH)

    edges = (
        lines.filter(~F.col("value").startswith("#"))
        .withColumn("parts", F.split(F.col("value"), r"\s+"))
        .select(
            F.col("parts")[0].cast("long").alias("src"),
            F.col("parts")[1].cast("long").alias("dst"),
        )
    )

    # Reused for both the in-degree aggregation and the broadcast join below.
    edges.cache()

    in_degree = edges.groupBy("dst").agg(F.count("*").alias("in_degree"))

    top50 = in_degree.orderBy(F.col("in_degree").desc()).limit(50).cache()

    print("=== Top 50 destination nodes by in-degree ===")
    top50.show(50, truncate=False)

    # Broadcast the small top-50 set to avoid shuffling the full edge list
    # when checking how many edges land on a hub node.
    hub = top50.select(F.col("dst").alias("hub_dst"))
    hub_edge_count = edges.join(F.broadcast(hub), edges.dst == hub.hub_dst, "inner").count()
    print(f"Edges landing on a top-50 hub node: {hub_edge_count}")

    total_nodes = in_degree.count()
    total_edges = edges.count()
    print(f"Total distinct destination vertices: {total_nodes}")
    print(f"Total edges parsed: {total_edges}")

    top50.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH)
    print(f"Top 50 written to {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
