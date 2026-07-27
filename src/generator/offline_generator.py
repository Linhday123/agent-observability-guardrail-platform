"""Offline Event Generator for the Agent Observability platform.

Generates three synthetic datasets — agents (SCD2), tools, and
agent_action_history — and writes them as Parquet files to the
MinIO "Source Landing" zone. agent_action_history intentionally
contains four controlled data-quality issues used later by the
Batch Data Pipeline (DP2): skew, high cardinality, schema
evolution, and duplicate records.

Usage:
    uv run python src/generator/offline_generator.py
"""

from __future__ import annotations

import io
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
import yaml


@dataclass
class GeneratorConfig:
    """Typed view over config.yaml, used by every generation function."""

    random_seed: int
    n_agents: int
    n_tools: int
    n_actions: int
    history_days: int
    top_tools_count: int
    top_tools_share: float
    schema_new_column: str
    schema_change_after_days: int
    duplicate_rate: float
    minio_endpoint_url: str
    minio_bucket: str
    minio_source_landing_prefix: str

    @classmethod
    def from_yaml(cls, path: str | Path) -> "GeneratorConfig":
        """Load and flatten config.yaml into a GeneratorConfig instance."""
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        return cls(
            random_seed=raw["generator"]["random_seed"],
            n_agents=raw["generator"]["n_agents"],
            n_tools=raw["generator"]["n_tools"],
            n_actions=raw["generator"]["n_actions"],
            history_days=raw["generator"]["history_days"],
            top_tools_count=raw["skew"]["top_tools_count"],
            top_tools_share=raw["skew"]["top_tools_share"],
            schema_new_column=raw["schema_evolution"]["new_column"],
            schema_change_after_days=raw["schema_evolution"]["change_after_days"],
            duplicate_rate=raw["duplicate"]["duplicate_rate"],
            minio_endpoint_url=raw["minio"]["endpoint_url"],
            minio_bucket=raw["minio"]["bucket"],
            minio_source_landing_prefix=raw["minio"]["source_landing_prefix"],
        )


def generate_tools(config: GeneratorConfig) -> pd.DataFrame:
    """Generate the tools dimension (flat, no SCD2 needed for tools)."""
    categories = ["file_io", "database", "web_search", "code_exec", "email", "shell"]
    rows = []
    for i in range(config.n_tools):
        rows.append(
            {
                "tool_id": f"tool_{i:03d}",
                "tool_name": f"tool-{categories[i % len(categories)]}-{i:03d}",
                "category": categories[i % len(categories)],
            }
        )
    return pd.DataFrame(rows)


def generate_agents(config: GeneratorConfig) -> pd.DataFrame:
    """Generate the agents dimension with real SCD2 history.

    ~30% of agents get a second row simulating a model version upgrade:
    the first row is closed (is_current=False, valid_to_ts set), the
    second row is open (is_current=True, valid_to_ts=None).
    """
    teams = ["platform", "safety", "coding-assistant", "support-bot"]
    risk_levels = ["low", "medium", "high"]
    models = ["gpt-4o", "claude-4-sonnet", "claude-4-opus", "llama-3.1-70b"]

    now = datetime.now(timezone.utc)
    rows = []
    for i in range(config.n_agents):
        agent_id = f"agent_{i:04d}"
        team = teams[i % len(teams)]
        risk = risk_levels[i % len(risk_levels)]
        created_at = now - timedelta(days=random.randint(60, 365))

        has_upgrade = random.random() < 0.30
        if has_upgrade:
            upgrade_at = created_at + timedelta(days=random.randint(10, 45))
            rows.append(
                {
                    "agent_key": f"{agent_id}_v1",
                    "agent_id": agent_id,
                    "team": team,
                    "risk_level": risk,
                    "model_version": models[i % len(models)],
                    "valid_from_ts": created_at,
                    "valid_to_ts": upgrade_at,
                    "is_current": False,
                }
            )
            rows.append(
                {
                    "agent_key": f"{agent_id}_v2",
                    "agent_id": agent_id,
                    "team": team,
                    "risk_level": risk,
                    "model_version": models[(i + 1) % len(models)],
                    "valid_from_ts": upgrade_at,
                    "valid_to_ts": None,
                    "is_current": True,
                }
            )
        else:
            rows.append(
                {
                    "agent_key": f"{agent_id}_v1",
                    "agent_id": agent_id,
                    "team": team,
                    "risk_level": risk,
                    "model_version": models[i % len(models)],
                    "valid_from_ts": created_at,
                    "valid_to_ts": None,
                    "is_current": True,
                }
            )
    return pd.DataFrame(rows)


def _weighted_tool_ids(config: GeneratorConfig, tools: pd.DataFrame) -> list[str]:
    """Build the per-row weight list that produces the skewed tool
    distribution: the top `top_tools_count` tools receive
    `top_tools_share` of total probability mass, the rest share the
    remainder evenly.
    """
    tool_ids = tools["tool_id"].tolist()
    top = tool_ids[: config.top_tools_count]
    rest = tool_ids[config.top_tools_count :]

    weights = {}
    for t in top:
        weights[t] = config.top_tools_share / len(top)
    for t in rest:
        weights[t] = (1 - config.top_tools_share) / max(len(rest), 1)
    return random.choices(
        population=list(weights.keys()),
        weights=list(weights.values()),
        k=config.n_actions,
    )


def generate_agent_action_history(
    config: GeneratorConfig, agents: pd.DataFrame, tools: pd.DataFrame
) -> pd.DataFrame:
    """Generate the agent_action_history fact table.

    Contains, by construction:
      - Skew: 2 tools receive ~75% of calls (Line 5).
      - High cardinality: action_id / run_id are near-unique UUIDs (Line 6).
      - Schema evolution info: rows carry a `called_at` timestamp used
        later by write_source_landing() to split old vs. recent files
        with different schemas (Line 7).
      - A `_is_duplicate_seed` flag marking rows that will be
        duplicated at write time (Line 8).
    """
    current_agents = agents.loc[agents["is_current"], "agent_id"].tolist()
    now = datetime.now(timezone.utc)

    tool_id_choices = _weighted_tool_ids(config, tools)
    rows = []
    for i in range(config.n_actions):
        called_at = now - timedelta(
            days=random.uniform(0, config.history_days),
            seconds=random.randint(0, 86_400),
        )
        row = {
            "action_id": str(uuid.uuid4()),
            "run_id": str(uuid.uuid4()),
            "agent_id": random.choice(current_agents),
            "tool_id": tool_id_choices[i],
            "called_at": called_at,
            "duration_ms": random.randint(20, 5000),
            "tokens_used": random.randint(50, 4000),
        }
        # Column name comes from config.yaml (schema_evolution.new_column),
        # never hardcoded, so renaming it in config.yaml is enough to
        # propagate everywhere this column is referenced.
        row[config.schema_new_column] = f"v{random.randint(1, 3)}"
        rows.append(row)
    return pd.DataFrame(rows)


def apply_duplicates(config: GeneratorConfig, df: pd.DataFrame) -> pd.DataFrame:
    """Re-insert an exact copy of `duplicate_rate` of rows, keeping the
    same action_id, to simulate at-least-once delivery duplicates
    (record duplication).
    """
    n_dupes = int(len(df) * config.duplicate_rate)
    dupes = df.sample(n=n_dupes, random_state=config.random_seed).copy()
    return pd.concat([df, dupes], ignore_index=True)


def ensure_bucket_exists(client, bucket: str) -> None:
    """Create the MinIO bucket if it does not already exist."""
    existing = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def _upload_parquet(client, bucket: str, key: str, df: pd.DataFrame) -> None:
    """Serialize a DataFrame to Parquet in memory and upload it to MinIO."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"  uploaded s3://{bucket}/{key} ({len(df)} rows)")


def write_source_landing(
    config: GeneratorConfig,
    client,
    agents: pd.DataFrame,
    tools: pd.DataFrame,
    actions: pd.DataFrame,
) -> None:
    """Write all three datasets to the Source Landing zone in MinIO.

    agent_action_history is split into two files by `called_at`:
    the "old" file (older than schema_change_after_days) is written
    WITHOUT the guardrail_policy_version column, producing genuine
    file-level schema evolution for the Batch Data Pipeline (DP2 / Apache Spark) to handle via mergeSchema.
    """
    prefix = config.minio_source_landing_prefix
    bucket = config.minio_bucket

    _upload_parquet(client, bucket, f"{prefix}/agents/agents.parquet", agents)
    _upload_parquet(client, bucket, f"{prefix}/tools/tools.parquet", tools)

    actions = apply_duplicates(config, actions)

    cutoff = datetime.now(timezone.utc) - timedelta(
        days=config.schema_change_after_days
    )
    old = actions.loc[actions["called_at"] < cutoff].drop(
        columns=[config.schema_new_column]
    )
    recent = actions.loc[actions["called_at"] >= cutoff]

    _upload_parquet(
        client,
        bucket,
        f"{prefix}/agent_action_history/part-old.parquet",
        old,
    )
    _upload_parquet(
        client,
        bucket,
        f"{prefix}/agent_action_history/part-recent.parquet",
        recent,
    )


def main() -> None:
    """Entry point: load config, generate all datasets, upload to MinIO."""
    config = GeneratorConfig.from_yaml(Path(__file__).parent / "config.yaml")
    random.seed(config.random_seed)

    print("Generating tools...")
    tools = generate_tools(config)

    print("Generating agents (SCD2)...")
    agents = generate_agents(config)

    print("Generating agent_action_history...")
    actions = generate_agent_action_history(config, agents, tools)

    client = boto3.client(
        "s3",
        endpoint_url=config.minio_endpoint_url,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    )
    ensure_bucket_exists(client, config.minio_bucket)

    print("Writing Parquet files to MinIO Source Landing...")
    write_source_landing(config, client, agents, tools, actions)

    print("Done.")


if __name__ == "__main__":
    main()