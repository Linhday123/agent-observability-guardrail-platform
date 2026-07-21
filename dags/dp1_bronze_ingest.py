"""DP1 — Ingest Source Landing into Bronze Raw Data, then validate it.

Two tasks, matching an extract-then-validate pattern:
1. ingest_minio_to_bronze: copies Parquet files from Source Landing into
   Bronze Raw Data, preserving the old/recent partition split so batch
   processing can still demonstrate real schema-evolution handling.
2. validate_bronze: runs Great Expectations checks on the Bronze files
   using the from_pandas() fluent API.
"""

from __future__ import annotations

import io
from datetime import datetime

import boto3
import great_expectations as ge
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.sdk import DAG, task
from botocore.config import Config

BUCKET = "lakehouse"
SOURCE_PREFIX = "source-landing"
BRONZE_PREFIX = "bronze"

SOURCE_TO_BRONZE = {
    f"{SOURCE_PREFIX}/agents/agents.parquet": f"{BRONZE_PREFIX}/raw_agents.parquet",
    f"{SOURCE_PREFIX}/tools/tools.parquet": f"{BRONZE_PREFIX}/raw_tools.parquet",
    f"{SOURCE_PREFIX}/agent_action_history/part-old.parquet": f"{BRONZE_PREFIX}/raw_agent_actions_old.parquet",
    f"{SOURCE_PREFIX}/agent_action_history/part-recent.parquet": f"{BRONZE_PREFIX}/raw_agent_actions_recent.parquet",
}


def _minio_client():
    """Build a boto3 S3 client using the Airflow Connection 'minio_default'.

    Forces path-style addressing (http://minio:9000/lakehouse/...) instead
    of virtual-host style (http://lakehouse.minio:9000/...), since the
    latter's hostname is not resolvable on the Docker network.
    """
    conn = BaseHook.get_connection("minio_default")
    return boto3.client(
        "s3",
        endpoint_url=conn.host,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _read_parquet(client, key: str) -> pd.DataFrame:
    obj = client.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")


@task
def ingest_minio_to_bronze() -> None:
    """Copy every Source Landing Parquet file into Bronze Raw Data."""
    client = _minio_client()
    for source_key, bronze_key in SOURCE_TO_BRONZE.items():
        obj = client.get_object(Bucket=BUCKET, Key=source_key)
        data = obj["Body"].read()
        client.put_object(Bucket=BUCKET, Key=bronze_key, Body=data)
        print(f"  copied s3://{BUCKET}/{source_key} -> s3://{BUCKET}/{bronze_key}")


def _validate_dataframe(df: pd.DataFrame, name: str, required_cols: list[str],
                         not_null_cols: list[str], unique_cols: list[str]) -> None:
    """Run a Great Expectations suite on one Bronze table: from_pandas()
    plus a chain of expectations plus validate(), raising on any failure."""
    ge_df = ge.from_pandas(df)

    ge_df.expect_table_row_count_to_be_between(min_value=1)

    for col in required_cols:
        ge_df.expect_column_to_exist(col)
    for col in not_null_cols:
        ge_df.expect_column_values_to_not_be_null(col)
    for col in unique_cols:
        ge_df.expect_column_values_to_be_unique(col)

    results = ge_df.validate()
    passed = sum(1 for r in results["results"] if r["success"])
    print(f"  [{name}] {passed}/{len(results['results'])} expectations passed")

    if not results["success"]:
        failed = [r["expectation_config"]["expectation_type"] for r in results["results"] if not r["success"]]
        raise ValueError(f"Data quality FAILED for {name}: {failed}")


@task
def validate_bronze() -> None:
    """Run Great Expectations checks on the Bronze files just written."""
    client = _minio_client()

    agents = _read_parquet(client, f"{BRONZE_PREFIX}/raw_agents.parquet")
    tools = _read_parquet(client, f"{BRONZE_PREFIX}/raw_tools.parquet")
    actions_old = _read_parquet(client, f"{BRONZE_PREFIX}/raw_agent_actions_old.parquet")
    actions_recent = _read_parquet(client, f"{BRONZE_PREFIX}/raw_agent_actions_recent.parquet")

    if actions_old.empty:
        raise ValueError("old actions partition is empty")

    if actions_recent.empty:
        raise ValueError("recent actions partition is empty")

    actions = pd.concat([actions_old, actions_recent], ignore_index=True)

    _validate_dataframe(
        agents,
        "raw_agents",
        required_cols=[
            "agent_key", "agent_id", "team", "risk_level", "model_version",
            "valid_from_ts", "valid_to_ts", "is_current",
        ],
        not_null_cols=["agent_id"],
        unique_cols=["agent_key"],
    )

    _validate_dataframe(
        tools,
        "raw_tools",
        required_cols=["tool_id", "tool_name", "category"],
        not_null_cols=["tool_id"],
        unique_cols=["tool_id"],
    )

    _validate_dataframe(
        actions,
        "raw_agent_actions",
        required_cols=[
            "action_id", "run_id", "agent_id", "tool_id", "called_at",
            "duration_ms", "tokens_used",
        ],
        not_null_cols=["action_id", "agent_id"],
        unique_cols=[],  # action_id intentionally has ~2% duplicates
    )

    if "guardrail_policy_version" not in actions_recent.columns:
        raise ValueError("recent partition missing guardrail_policy_version")

    if "guardrail_policy_version" in actions_old.columns:
        raise ValueError("old partition unexpectedly has guardrail_policy_version")

    print(
        "  Schema-evolution check passed: "
        "old/recent partitions differ as expected."
    )


with DAG(
    dag_id="dp1_bronze_ingest",
    description="Ingest Source Landing into Bronze Raw Data, then validate with Great Expectations",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dp1", "bronze", "great-expectations"],
) as dag:
    ingest_minio_to_bronze() >> validate_bronze()
