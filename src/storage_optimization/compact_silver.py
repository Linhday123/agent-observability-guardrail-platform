"""Compact Silver agent actions from multiple Parquet files into one file."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

SOURCE_PATH = f"s3a://{BUCKET}/silver/stg_agent_actions"
TARGET_PATH = f"s3a://{BUCKET}/silver/compacted/stg_agent_actions"


def build_spark() -> SparkSession:
    """Create a local Spark session configured for MinIO."""
    return (
        SparkSession.builder
        .appName("silver-compaction")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main() -> None:
    """Read bucketed Silver data and compact it into one Parquet file."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source = spark.read.parquet(SOURCE_PATH)

    before_files = len(source.inputFiles())
    row_count = source.count()

    (
        source.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(TARGET_PATH)
    )

    compacted = spark.read.parquet(TARGET_PATH)
    after_files = len(compacted.inputFiles())
    compacted_rows = compacted.count()

    print(
        "[compaction] "
        f"before_files={before_files}, "
        f"after_files={after_files}, "
        f"before_rows={row_count}, "
        f"after_rows={compacted_rows}"
    )

    if row_count != compacted_rows:
        raise RuntimeError(
            f"Row count changed during compaction: "
            f"{row_count} != {compacted_rows}"
        )

    if after_files >= before_files:
        raise RuntimeError(
            f"Compaction did not reduce files: "
            f"{before_files} -> {after_files}"
        )

    print("[compaction validation] PASS")

    spark.stop()


if __name__ == "__main__":
    main()
