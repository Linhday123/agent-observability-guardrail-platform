"""Optimized batch processing over Bronze Raw Data.

Fixes the four data-quality problems carried through from Bronze:
  - schema evolution: merge schemas across old/recent Parquet files
  - duplicate: deduplicate by action_id, keeping the latest called_at
  - skew: manual salting balances the skewed groupBy on tool_id
  - high cardinality: bucketBy instead of partitionBy avoids one tiny
    file per near-unique action_id

Note: this script processes the full dataset (dedup, both aggregation
phases, all three Silver outputs), so its total elapsed time is not
directly comparable to batch_baseline.py, which only writes a 1,000-row
sample.
"""

from __future__ import annotations

import argparse
import os
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
SALT_BUCKETS = 8


def build_spark(app_name: str) -> SparkSession:
    """Build a SparkSession configured to read/write MinIO via S3A.

    AQE is enabled for general shuffle-partition coalescing, but its
    skew-join optimizer only applies to joins, not plain aggregations —
    the groupBy skew below is fixed with manual salting instead. The
    warehouse/metastore/derby-log locations are pointed at /tmp so no
    Spark-generated artifacts land in the repository; actual table data
    is still written to MinIO via an explicit external table path.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dderby.system.home=/tmp/derby-metastore -Dderby.stream.error.file=/tmp/derby.log",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[spark] UI: {spark.sparkContext.uiWebUrl}")
    return spark


def run(bronze_prefix: str = "bronze", silver_prefix: str = "silver") -> None:
    """Run the full optimized DP2 batch job."""
    spark = build_spark("batch-optimized")
    start = time.time()

    # --- Schema evolution: mergeSchema unifies old/recent partitions. ---
    actions = spark.read.option("mergeSchema", "true").parquet(
        f"s3a://{BUCKET}/{bronze_prefix}/raw_agent_actions_old.parquet",
        f"s3a://{BUCKET}/{bronze_prefix}/raw_agent_actions_recent.parquet",
    )
    print(f"[optimized] actions schema after mergeSchema: {actions.columns}")

    # --- Duplicate: keep only the latest row per action_id. persist()
    # avoids recomputing this window function on every reference below. ---
    window = Window.partitionBy("action_id").orderBy(F.col("called_at").desc())
    deduped = (
        actions.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    before, after = actions.count(), deduped.count()
    print(f"[optimized] rows before dedup: {before}, after dedup: {after}")

    # --- Skew: manual salting with a deterministic hash (xxhash64), so
    # the same input produces the same distribution across runs.
    # partial_counts is persisted since it is used twice below. ---
    salted = deduped.withColumn(
        "_salt", F.pmod(F.xxhash64("action_id"), F.lit(SALT_BUCKETS))
    )
    partial_counts = (
        salted.groupBy("tool_id", "_salt").count().persist(StorageLevel.MEMORY_AND_DISK)
    )
    print("[optimized] per (tool_id, salt bucket) row counts:")
    partial_counts.orderBy(F.col("count").desc()).show(20, truncate=False)

    per_tool = (
        partial_counts.groupBy("tool_id")
        .agg(F.sum("count").alias("count"))
        .orderBy(F.col("count").desc())
    )
    per_tool.show(12, truncate=False)
    partial_counts.unpersist()

    # --- High cardinality: bucketBy instead of partitionBy avoids one
    # tiny file per near-unique action_id. option("path", ...) makes
    # this an external table so the data lives on MinIO, not in the
    # local /tmp warehouse (only table metadata stays local). format()
    # is explicit rather than relying on Spark's default. mode(
    # "overwrite") handles re-running safely without a manual DROP TABLE. ---
    target_path = f"s3a://{BUCKET}/{silver_prefix}/stg_agent_actions"
    (
        deduped.write.format("parquet")
        .mode("overwrite")
        .bucketBy(8, "action_id")
        .sortBy("action_id")
        .option("path", target_path)
        .saveAsTable("stg_agent_actions")
    )

    silver_actions = spark.read.parquet(target_path)
    observed_files = len(silver_actions.inputFiles())
    print(
        "[optimized] Silver validation: "
        f"rows={silver_actions.count()}, observed_files={observed_files}"
    )

    # Dimensions copied through unchanged, as Silver Clean Data.
    agents = spark.read.parquet(f"s3a://{BUCKET}/{bronze_prefix}/raw_agents.parquet")
    tools = spark.read.parquet(f"s3a://{BUCKET}/{bronze_prefix}/raw_tools.parquet")
    agents.write.mode("overwrite").parquet(f"s3a://{BUCKET}/{silver_prefix}/stg_agents.parquet")
    tools.write.mode("overwrite").parquet(f"s3a://{BUCKET}/{silver_prefix}/stg_tools.parquet")

    deduped.unpersist()

    elapsed = time.time() - start
    print(f"[optimized] total elapsed: {elapsed:.2f}s (full dataset — not comparable to baseline)")

    hold_seconds = int(os.getenv("SPARK_UI_HOLD_SECONDS", "0"))
    if hold_seconds > 0:
        print(f"[spark] keeping Spark UI alive for {hold_seconds}s: {spark.sparkContext.uiWebUrl}")
        time.sleep(hold_seconds)

    spark.stop()


def main() -> None:
    """CLI entry point so a scheduler can invoke this script directly.
    This only proves the script is scheduler-ready (CLI args) — it does
    not by itself prove a scheduler has actually invoked it."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-prefix", default="bronze")
    parser.add_argument("--silver-prefix", default="silver")
    args = parser.parse_args()
    run(bronze_prefix=args.bronze_prefix, silver_prefix=args.silver_prefix)


if __name__ == "__main__":
    main()
