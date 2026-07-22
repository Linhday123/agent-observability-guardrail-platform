"""DP2: Bronze -> Silver with Spark Local, then Silver -> PostgreSQL Gold.

Task 1:
- Invokes the optimized Spark job from Day 5.
- Reads Silver Parquet from MinIO.
- Resolves the correct SCD2 agent_key for every action.
- Derives a deterministic is_violation flag.
- Loads dim_agent, dim_tool, and fact_agent_action.

Task 2:
- Validates row counts, foreign-key relationships, SCD2 mapping,
  required fields, and the generated violation rate.
"""

from __future__ import annotations

import hashlib
import io
import os
import subprocess
from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import dag, task


def _required_environment() -> None:
    """Fail early when a required connection setting is missing."""
    required = [
        "MINIO_ENDPOINT",
        "MINIO_BUCKET",
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "POSTGRES_GOLD_HOST",
        "POSTGRES_GOLD_PORT",
        "POSTGRES_GOLD_DB",
        "POSTGRES_GOLD_USER",
        "POSTGRES_GOLD_PASSWORD",
    ]

    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {missing}")


def _s3_client():
    """Build a boto3 client for MinIO."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _read_parquet_prefix(prefix: str):
    """Read all Parquet objects under one MinIO prefix into pandas."""
    import pandas as pd
    import pyarrow.parquet as pq

    client = _s3_client()
    bucket = os.environ["MINIO_BUCKET"]

    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(
            obj["Key"]
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        )

    keys = sorted(keys)

    if not keys:
        raise RuntimeError(
            f"No Parquet objects found at s3://{bucket}/{prefix}"
        )

    frames = []

    for key in keys:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        frames.append(table.to_pandas())

    result = pd.concat(frames, ignore_index=True)

    print(
        f"[minio] prefix={prefix}, "
        f"parquet_files={len(keys)}, rows={len(result)}"
    )

    return result


def _require_columns(
    dataset_name: str,
    dataframe,
    required_columns: set[str],
) -> None:
    """Check that a Silver dataset exposes all required columns."""
    missing = required_columns.difference(dataframe.columns)

    if missing:
        raise RuntimeError(
            f"{dataset_name} is missing required columns: {sorted(missing)}"
        )


def _derive_is_violation(action_id: str) -> bool:
    """Derive a reproducible guardrail violation flag.

    This coursework source data does not contain a native violation flag.
    A stable SHA-256 rule produces approximately an 8% violation rate.
    Python's built-in hash() is intentionally not used because its result
    may differ between processes.
    """
    digest = hashlib.sha256(action_id.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100 < 8


def _python_timestamp(value: Any):
    """Convert pandas timestamps/NaT into psycopg2-safe values."""
    import pandas as pd

    if pd.isna(value):
        return None

    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()

    return value


def _nullable_text(value: Any) -> str | None:
    """Convert pandas null-like values into SQL NULL."""
    import pandas as pd

    if pd.isna(value):
        return None

    return str(value)


def _nullable_integer(value: Any) -> int | None:
    """Convert pandas nullable integer values into Python int/None."""
    import pandas as pd

    if pd.isna(value):
        return None

    return int(value)


def _postgres_connection():
    """Open a connection to PostgreSQL Gold Warehouse."""
    import psycopg2

    return psycopg2.connect(
        host=os.environ["POSTGRES_GOLD_HOST"],
        port=os.environ["POSTGRES_GOLD_PORT"],
        dbname=os.environ["POSTGRES_GOLD_DB"],
        user=os.environ["POSTGRES_GOLD_USER"],
        password=os.environ["POSTGRES_GOLD_PASSWORD"],
    )


@dag(
    dag_id="dp2_silver_gold",
    description="Spark Bronze-to-Silver processing and Silver-to-Gold loading",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "vanlinh",
        "retries": 0,
    },
    tags=["dp2", "spark", "silver", "gold"],
)
def dp2_silver_gold():
    """Build and validate the PostgreSQL Gold Model."""

    @task(
        task_id="spark_bronze_to_silver_gold",
        execution_timeout=timedelta(minutes=20),
    )
    def spark_bronze_to_silver_gold() -> dict[str, int | float]:
        """Run Spark Local and load all Silver datasets into Gold."""
        import pandas as pd
        from psycopg2.extras import execute_values

        _required_environment()

        spark_command = [
            "python",
            "/opt/airflow/dags/batch_processing/batch_optimized.py",
            "--bronze-prefix",
            "bronze",
            "--silver-prefix",
            "silver",
        ]

        print("[dp2] Starting Spark Local optimized batch job")
        subprocess.run(
            spark_command,
            check=True,
            env=os.environ.copy(),
        )
        print("[dp2] Spark Bronze -> Silver completed")

        agents = _read_parquet_prefix("silver/stg_agents.parquet")
        tools = _read_parquet_prefix("silver/stg_tools.parquet")
        actions = _read_parquet_prefix("silver/stg_agent_actions")

        _require_columns(
            "stg_agents",
            agents,
            {
                "agent_key",
                "agent_id",
                "team",
                "risk_level",
                "model_version",
                "valid_from_ts",
                "valid_to_ts",
                "is_current",
            },
        )

        _require_columns(
            "stg_tools",
            tools,
            {"tool_id", "tool_name", "category"},
        )

        _require_columns(
            "stg_agent_actions",
            actions,
            {
                "action_id",
                "run_id",
                "agent_id",
                "tool_id",
                "called_at",
                "duration_ms",
                "tokens_used",
                "guardrail_policy_version",
            },
        )

        if agents["agent_key"].duplicated().any():
            raise RuntimeError("Duplicate agent_key found in stg_agents")

        if tools["tool_id"].duplicated().any():
            raise RuntimeError("Duplicate tool_id found in stg_tools")

        if actions["action_id"].duplicated().any():
            raise RuntimeError("Duplicate action_id found in Silver actions")

        agents["valid_from_ts"] = pd.to_datetime(
            agents["valid_from_ts"],
            utc=True,
            errors="raise",
        )
        agents["valid_to_ts"] = pd.to_datetime(
            agents["valid_to_ts"],
            utc=True,
            errors="coerce",
        )
        actions["called_at"] = pd.to_datetime(
            actions["called_at"],
            utc=True,
            errors="raise",
        )
        

                # Some historical actions occur before the first known SCD2
        # version of their agent. Backdate only the earliest version
        # to the first observed action timestamp so every historical
        # fact can resolve to a valid agent_key.
        first_action_by_agent = (
            actions.groupby("agent_id")["called_at"].min()
        )

        earliest_version_indexes = (
            agents.groupby("agent_id")["valid_from_ts"].idxmin()
        )

        backdated_version_count = 0

        for row_index in earliest_version_indexes:
            agent_id = agents.at[row_index, "agent_id"]
            earliest_action = first_action_by_agent.get(agent_id)
            current_valid_from = agents.at[row_index, "valid_from_ts"]

            if (
                pd.notna(earliest_action)
                and earliest_action < current_valid_from
            ):
                agents.at[row_index, "valid_from_ts"] = earliest_action
                backdated_version_count += 1

        print(
            "[dp2] SCD2 history alignment: "
            f"backdated_earliest_versions={backdated_version_count}"
        )

        # Temporal SCD2 join:
        # valid_from_ts <= called_at < valid_to_ts
        candidate_matches = actions.merge(
            agents[
                [
                    "agent_key",
                    "agent_id",
                    "valid_from_ts",
                    "valid_to_ts",
                ]
            ],
            on="agent_id",
            how="left",
            validate="many_to_many",
        )

        is_valid_version = (
            candidate_matches["called_at"]
            >= candidate_matches["valid_from_ts"]
        ) & (
            candidate_matches["valid_to_ts"].isna()
            | (
                candidate_matches["called_at"]
                < candidate_matches["valid_to_ts"]
            )
        )

        matched_actions = candidate_matches.loc[is_valid_version].copy()

        match_counts = matched_actions.groupby("action_id").size()

        unmatched_count = int(
            (~actions["action_id"].isin(match_counts.index)).sum()
        )
        ambiguous_count = int((match_counts != 1).sum())

        if unmatched_count:
            raise RuntimeError(
                f"{unmatched_count} actions did not match an SCD2 agent version"
            )

        if ambiguous_count:
            raise RuntimeError(
                f"{ambiguous_count} actions matched multiple SCD2 versions"
            )

        if len(matched_actions) != len(actions):
            raise RuntimeError(
                "Temporal join changed the action row count unexpectedly"
            )

        facts = matched_actions[
            [
                "action_id",
                "run_id",
                "agent_key",
                "tool_id",
                "called_at",
                "duration_ms",
                "tokens_used",
                "guardrail_policy_version",
            ]
        ].copy()

        facts["is_violation"] = facts["action_id"].map(
            _derive_is_violation
        )

        unknown_tools = sorted(
            set(facts["tool_id"]) - set(tools["tool_id"])
        )

        if unknown_tools:
            raise RuntimeError(
                f"Actions reference unknown tools: {unknown_tools[:10]}"
            )

        dim_agent_rows = [
            (
                str(agent_key),
                str(agent_id),
                str(team),
                str(risk_level),
                str(model_version),
                _python_timestamp(valid_from_ts),
                _python_timestamp(valid_to_ts),
                bool(is_current),
            )
            for (
                agent_key,
                agent_id,
                team,
                risk_level,
                model_version,
                valid_from_ts,
                valid_to_ts,
                is_current,
            ) in agents[
                [
                    "agent_key",
                    "agent_id",
                    "team",
                    "risk_level",
                    "model_version",
                    "valid_from_ts",
                    "valid_to_ts",
                    "is_current",
                ]
            ].itertuples(index=False, name=None)
        ]

        dim_tool_rows = [
            (
                str(tool_id),
                str(tool_name),
                str(category),
            )
            for tool_id, tool_name, category in tools[
                ["tool_id", "tool_name", "category"]
            ].itertuples(index=False, name=None)
        ]

        fact_rows = [
            (
                str(action_id),
                str(run_id),
                str(agent_key),
                str(tool_id),
                _python_timestamp(called_at),
                int(duration_ms),
                _nullable_integer(tokens_used),
                _nullable_text(guardrail_policy_version),
                bool(is_violation),
            )
            for (
                action_id,
                run_id,
                agent_key,
                tool_id,
                called_at,
                duration_ms,
                tokens_used,
                guardrail_policy_version,
                is_violation,
            ) in facts[
                [
                    "action_id",
                    "run_id",
                    "agent_key",
                    "tool_id",
                    "called_at",
                    "duration_ms",
                    "tokens_used",
                    "guardrail_policy_version",
                    "is_violation",
                ]
            ].itertuples(index=False, name=None)
        ]

        connection = _postgres_connection()

        try:
            with connection:
                with connection.cursor() as cursor:
                    # Truncate all related tables together so the DAG
                    # remains safely re-runnable.
                    cursor.execute(
                        """
                        TRUNCATE TABLE
                            fact_agent_action,
                            dim_agent,
                            dim_tool
                        """
                    )

                    execute_values(
                        cursor,
                        """
                        INSERT INTO dim_agent (
                            agent_key,
                            agent_id,
                            team,
                            risk_level,
                            model_version,
                            valid_from_ts,
                            valid_to_ts,
                            is_current
                        ) VALUES %s
                        """,
                        dim_agent_rows,
                        page_size=1000,
                    )

                    execute_values(
                        cursor,
                        """
                        INSERT INTO dim_tool (
                            tool_id,
                            tool_name,
                            category
                        ) VALUES %s
                        """,
                        dim_tool_rows,
                        page_size=1000,
                    )

                    execute_values(
                        cursor,
                        """
                        INSERT INTO fact_agent_action (
                            action_id,
                            run_id,
                            agent_key,
                            tool_id,
                            called_at,
                            duration_ms,
                            tokens_used,
                            guardrail_policy_version,
                            is_violation
                        ) VALUES %s
                        """,
                        fact_rows,
                        page_size=5000,
                    )
        finally:
            connection.close()

        violation_rate = float(facts["is_violation"].mean())

        summary: dict[str, int | float] = {
            "dim_agent_rows": len(dim_agent_rows),
            "dim_tool_rows": len(dim_tool_rows),
            "fact_agent_action_rows": len(fact_rows),
            "violation_rate": violation_rate,
        }

        print(f"[dp2] Gold load summary: {summary}")
        return summary

    @task(task_id="validate_silver_gold")
    def validate_silver_gold(
        expected: dict[str, int | float],
    ) -> None:
        """Validate Gold row counts, SCD2 mapping and relationships."""
        connection = _postgres_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM dim_agent")
                dim_agent_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM dim_tool")
                dim_tool_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM fact_agent_action")
                fact_count = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_agent_action fact
                    LEFT JOIN dim_agent agent
                        ON fact.agent_key = agent.agent_key
                    WHERE agent.agent_key IS NULL
                    """
                )
                orphan_agent_count = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_agent_action fact
                    LEFT JOIN dim_tool tool
                        ON fact.tool_id = tool.tool_id
                    WHERE tool.tool_id IS NULL
                    """
                )
                orphan_tool_count = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_agent_action fact
                    JOIN dim_agent agent
                        ON fact.agent_key = agent.agent_key
                    WHERE fact.called_at < agent.valid_from_ts
                       OR (
                           agent.valid_to_ts IS NOT NULL
                           AND fact.called_at >= agent.valid_to_ts
                       )
                    """
                )
                invalid_scd2_mapping_count = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT agent_id
                        FROM dim_agent
                        GROUP BY agent_id
                        HAVING COUNT(*) FILTER (WHERE is_current) <> 1
                    ) invalid_agents
                    """
                )
                invalid_current_version_count = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT
                        AVG(
                            CASE WHEN is_violation
                                 THEN 1.0
                                 ELSE 0.0
                            END
                        )
                    FROM fact_agent_action
                    """
                )
                violation_rate = float(cursor.fetchone()[0] or 0.0)
        finally:
            connection.close()

        assert dim_agent_count == expected["dim_agent_rows"], (
            f"dim_agent count mismatch: "
            f"{dim_agent_count} != {expected['dim_agent_rows']}"
        )

        assert dim_tool_count == expected["dim_tool_rows"], (
            f"dim_tool count mismatch: "
            f"{dim_tool_count} != {expected['dim_tool_rows']}"
        )

        assert fact_count == expected["fact_agent_action_rows"], (
            f"fact count mismatch: "
            f"{fact_count} != {expected['fact_agent_action_rows']}"
        )

        assert orphan_agent_count == 0, (
            f"Found {orphan_agent_count} orphan agent foreign keys"
        )

        assert orphan_tool_count == 0, (
            f"Found {orphan_tool_count} orphan tool foreign keys"
        )

        assert invalid_scd2_mapping_count == 0, (
            f"Found {invalid_scd2_mapping_count} invalid SCD2 mappings"
        )

        assert invalid_current_version_count == 0, (
            f"Found {invalid_current_version_count} agents without "
            "exactly one current version"
        )

        assert 0.01 <= violation_rate <= 0.20, (
            f"Unexpected generated violation rate: {violation_rate:.4f}"
        )

        print(
            "[dp2 validation] PASS — "
            f"dim_agent={dim_agent_count}, "
            f"dim_tool={dim_tool_count}, "
            f"fact_agent_action={fact_count}, "
            f"violation_rate={violation_rate:.4%}, "
            "orphans=0, invalid_scd2_mappings=0"
        )

    load_summary = spark_bronze_to_silver_gold()
    validate_silver_gold(load_summary)


dp2_silver_gold()
