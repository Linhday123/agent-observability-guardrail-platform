"""Data quality report for the Offline Event Generator output.

Downloads the Parquet files just written to MinIO Source Landing and
computes real metrics proving the four intentional data-quality issues:
skew, high cardinality, schema evolution, and duplicates. This is a
round-trip check (read back from MinIO, not the in-memory DataFrame)
so the numbers reflect what was actually persisted.

Usage:
    uv run python src/generator/quality_report.py
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import boto3
import pandas as pd
import yaml


def load_minio_config(path: str | Path) -> dict:
    """Load only the `minio` section of config.yaml."""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return raw["minio"]


def _download_parquet(client, bucket: str, key: str) -> pd.DataFrame:
    """Download a single Parquet object from MinIO into a DataFrame."""
    obj = client.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(obj["Body"].read())
    return pd.read_parquet(buffer, engine="pyarrow")


def report_skew(actions: pd.DataFrame) -> None:
    """Print the share of calls owned by the top 2 tools (data skew)."""
    counts = actions["tool_id"].value_counts(normalize=True)
    top2_share = counts.head(2).sum()
    print("\n[Skew] Top 2 tools' share of total calls:")
    print(counts.head(5).to_string())
    print(f"  -> top 2 tools = {top2_share:.1%} of all calls")


def report_cardinality(actions: pd.DataFrame) -> None:
    """Print uniqueness ratio of action_id / run_id (high cardinality)."""
    n = len(actions)
    n_unique_action = actions["action_id"].nunique()
    n_unique_run = actions["run_id"].nunique()
    print("\n[High cardinality] Uniqueness ratio:")
    print(f"  action_id: {n_unique_action}/{n} unique ({n_unique_action / n:.1%})")
    print(f"  run_id:    {n_unique_run}/{n} unique ({n_unique_run / n:.1%})")


def report_schema_evolution(old: pd.DataFrame, recent: pd.DataFrame) -> None:
    """Print the column-set difference between the old and recent
    partitions of agent_action_history (schema evolution)."""
    print("\n[Schema evolution] Column comparison between partitions:")
    print(f"  part-old.parquet columns:    {sorted(old.columns)}")
    print(f"  part-recent.parquet columns: {sorted(recent.columns)}")
    missing = set(recent.columns) - set(old.columns)
    print(f"  -> columns present only in the recent partition: {sorted(missing)}")


def report_duplicates(actions: pd.DataFrame) -> None:
    """Print the duplicate rate of action_id (record duplication)."""
    n = len(actions)
    n_unique = actions["action_id"].nunique()
    dup_count = n - n_unique
    print("\n[Duplicate] action_id duplication:")
    print(f"  total rows: {n}, unique action_id: {n_unique}")
    print(f"  duplicate rows: {dup_count} ({dup_count / n:.1%})")


def main() -> None:
    """Entry point: download from MinIO, then print all four reports."""
    minio_cfg = load_minio_config(Path(__file__).parent / "config.yaml")
    client = boto3.client(
        "s3",
        endpoint_url=minio_cfg["endpoint_url"],
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    )
    bucket = minio_cfg["bucket"]
    prefix = minio_cfg["source_landing_prefix"]

    old = _download_parquet(
        client, bucket, f"{prefix}/agent_action_history/part-old.parquet"
    )
    recent = _download_parquet(
        client, bucket, f"{prefix}/agent_action_history/part-recent.parquet"
    )
    actions = pd.concat([old, recent], ignore_index=True)

    print("=" * 60)
    print("DATA QUALITY REPORT — agent_action_history (Source Landing)")
    print("=" * 60)
    report_skew(actions)
    report_cardinality(actions)
    report_schema_evolution(old, recent)
    report_duplicates(actions)
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()