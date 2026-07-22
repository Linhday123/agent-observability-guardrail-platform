"""Baseline batch processing over Bronze Raw Data (unoptimized).

Reads agent_action_history from Bronze without addressing any of the
four seeded data-quality problems, so their effects are directly
observable in the Spark UI. This is the unoptimized contrast case used
to demonstrate the correctness and data-layout improvements made by
batch_optimized.py. The total runtimes of the two scripts are not a
direct performance comparison, since they write different output
volumes.
"""

from __future__ import annotations

import os
import time

from pyspark.sql import SparkSession

BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


def build_spark(app_name: str) -> SparkSession:
    """Build a SparkSession configured to read/write MinIO via S3A.

    shuffle.partitions is capped at 8 (instead of the default 200) so
    skew and high-cardinality effects stay visible on a modest local
    dataset. The warehouse/metastore/derby-log locations are pointed at
    /tmp so no Spark-generated artifacts land in the repo.
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
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[spark] UI: {spark.sparkContext.uiWebUrl}")
    return spark


def main() -> None:
    """Run the unoptimized baseline and print elapsed time."""
    spark = build_spark("batch-baseline")
    start = time.time()

    # Schema evolution problem: reading old + recent WITHOUT mergeSchema.
    actions = spark.read.parquet(
        f"s3a://{BUCKET}/bronze/raw_agent_actions_old.parquet",
        f"s3a://{BUCKET}/bronze/raw_agent_actions_recent.parquet",
    )
    print(f"[baseline] actions schema: {actions.columns}")
    if "guardrail_policy_version" not in actions.columns:
        print("[baseline] schema evolution issue observed: guardrail_policy_version is missing")
    else:
        print(
            "[baseline] guardrail_policy_version was inferred; baseline still "
            "does not explicitly guarantee schema merging across all files"
        )
    print(f"[baseline] actions row count (with duplicates): {actions.count()}")

    # Skew problem: two tools dominate ~75% of calls.
    per_tool = actions.groupBy("tool_id").count().orderBy("count", ascending=False)
    per_tool.show(12, truncate=False)

    # High cardinality problem: partitioning by a near-unique column
    # creates one tiny dataset per distinct value. A 1,000-row sample
    # keeps the S3 PUT request count manageable. Written under
    # benchmark/, not silver/, since this is a deliberately bad
    # anti-pattern output, not real Silver Clean Data.
    baseline_sample = actions.limit(1000)
    (
        baseline_sample.write.mode("overwrite")
        .partitionBy("action_id")
        .parquet(f"s3a://{BUCKET}/benchmark/stg_agent_actions_partitioned_by_action_id/")
    )

    elapsed = time.time() - start
    print(f"[baseline] total elapsed: {elapsed:.2f}s (1,000-row sample only — not comparable to optimized)")

    hold_seconds = int(os.getenv("SPARK_UI_HOLD_SECONDS", "0"))
    if hold_seconds > 0:
        print(f"[spark] keeping Spark UI alive for {hold_seconds}s: {spark.sparkContext.uiWebUrl}")
        time.sleep(hold_seconds)

    spark.stop()


if __name__ == "__main__":
    main()
