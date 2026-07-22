"""DP3: Compute and validate the seven-day agent guardrail violation rate.

The feature is computed from PostgreSQL Gold:
- fact_agent_action supplies event time and is_violation.
- dim_agent resolves SCD2 agent_key back to the business agent_id.

The seven-day window is anchored to MAX(called_at), rather than the
server clock, so the computation remains reproducible for coursework data.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


FEATURE_TABLE = "feat_agent_guardrail_violation_rate_7d"


def _required_environment() -> None:
    """Fail early if PostgreSQL Gold settings are missing."""
    required = [
        "POSTGRES_GOLD_HOST",
        "POSTGRES_GOLD_PORT",
        "POSTGRES_GOLD_DB",
        "POSTGRES_GOLD_USER",
        "POSTGRES_GOLD_PASSWORD",
    ]

    missing = [name for name in required if not os.getenv(name)]

    if missing:
        raise RuntimeError(
            f"Missing required PostgreSQL environment variables: {missing}"
        )


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
    dag_id="dp3_offline_feature",
    description="Compute and validate the seven-day guardrail violation feature",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "vanlinh",
        "retries": 0,
    },
    tags=["dp3", "offline-feature", "gold", "validation"],
)
def dp3_offline_feature():
    """Build and validate the DP3 offline feature table."""

    @task(
        task_id="compute_load_offline_feature",
        execution_timeout=timedelta(minutes=5),
    )
    def compute_load_offline_feature() -> dict[str, int | float | str]:
        """Compute one seven-day feature snapshot per active agent."""
        _required_environment()
        connection = _postgres_connection()

        try:
            with connection:
                with connection.cursor() as cursor:
                    # The table contains the latest materialized feature
                    # snapshot. event_timestamp is the feature observation
                    # time; created is the warehouse load timestamp.
                    cursor.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {FEATURE_TABLE} (
                            agent_id            TEXT PRIMARY KEY,
                            violation_count     INTEGER NOT NULL,
                            action_count        INTEGER NOT NULL,
                            violation_rate_7d   DOUBLE PRECISION NOT NULL,
                            event_timestamp     TIMESTAMPTZ NOT NULL,
                            created             TIMESTAMPTZ NOT NULL
                                                DEFAULT CURRENT_TIMESTAMP,

                            CONSTRAINT chk_feature_action_count
                                CHECK (action_count > 0),

                            CONSTRAINT chk_feature_violation_count
                                CHECK (
                                    violation_count >= 0
                                    AND violation_count <= action_count
                                ),

                            CONSTRAINT chk_feature_violation_rate
                                CHECK (
                                    violation_rate_7d >= 0
                                    AND violation_rate_7d <= 1
                                )
                        )
                        """
                    )

                    # Use the newest event in Gold as a reproducible anchor.
                    cursor.execute(
                        """
                        SELECT MAX(called_at)
                        FROM fact_agent_action
                        """
                    )
                    event_timestamp = cursor.fetchone()[0]

                    if event_timestamp is None:
                        raise RuntimeError(
                            "fact_agent_action is empty; DP3 cannot compute features"
                        )

                    window_start = event_timestamp - timedelta(days=7)

                    # This table stores the latest materialized snapshot,
                    # therefore reruns replace the previous snapshot.
                    cursor.execute(f"TRUNCATE TABLE {FEATURE_TABLE}")

                    cursor.execute(
                        f"""
                        WITH windowed_actions AS (
                            SELECT
                                agent.agent_id,
                                fact.is_violation
                            FROM fact_agent_action AS fact
                            JOIN dim_agent AS agent
                              ON fact.agent_key = agent.agent_key
                            WHERE fact.called_at > %s
                              AND fact.called_at <= %s
                        ),
                        aggregated AS (
                            SELECT
                                agent_id,
                                COUNT(*) FILTER (
                                    WHERE is_violation
                                )::INTEGER AS violation_count,
                                COUNT(*)::INTEGER AS action_count
                            FROM windowed_actions
                            GROUP BY agent_id
                        )
                        INSERT INTO {FEATURE_TABLE} (
                            agent_id,
                            violation_count,
                            action_count,
                            violation_rate_7d,
                            event_timestamp,
                            created
                        )
                        SELECT
                            agent_id,
                            violation_count,
                            action_count,
                            violation_count::DOUBLE PRECISION
                                / action_count::DOUBLE PRECISION,
                            %s,
                            CURRENT_TIMESTAMP
                        FROM aggregated
                        ORDER BY agent_id
                        """,
                        (
                            window_start,
                            event_timestamp,
                            event_timestamp,
                        ),
                    )

                    inserted_rows = cursor.rowcount

                    cursor.execute(
                        f"""
                        SELECT
                            COUNT(*) AS feature_rows,
                            SUM(action_count) AS window_action_count,
                            SUM(violation_count) AS window_violation_count,
                            AVG(violation_rate_7d) AS average_agent_rate
                        FROM {FEATURE_TABLE}
                        """
                    )

                    (
                        feature_rows,
                        window_action_count,
                        window_violation_count,
                        average_agent_rate,
                    ) = cursor.fetchone()

        finally:
            connection.close()

        if inserted_rows <= 0:
            raise RuntimeError(
                "DP3 produced no offline feature rows"
            )

        summary: dict[str, int | float | str] = {
            "feature_rows": int(feature_rows),
            "window_action_count": int(window_action_count or 0),
            "window_violation_count": int(window_violation_count or 0),
            "average_agent_rate": float(average_agent_rate or 0.0),
            "window_start": window_start.isoformat(),
            "event_timestamp": event_timestamp.isoformat(),
        }

        print(
            "[dp3] Offline feature load summary — "
            f"feature_rows={summary['feature_rows']}, "
            f"window_action_count={summary['window_action_count']}, "
            f"window_violation_count={summary['window_violation_count']}, "
            f"average_agent_rate={summary['average_agent_rate']:.4%}, "
            f"window_start={summary['window_start']}, "
            f"event_timestamp={summary['event_timestamp']}"
        )

        return summary

    @task(task_id="validate_feature")
    def validate_feature(
        expected: dict[str, int | float | str],
    ) -> None:
        """Validate DP3 row counts, timestamps and feature constraints."""
        _required_environment()
        connection = _postgres_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        COUNT(*) AS feature_rows,
                        COUNT(DISTINCT agent_id) AS distinct_agents,
                        COUNT(DISTINCT event_timestamp)
                            AS distinct_event_timestamps,

                        COUNT(*) FILTER (
                            WHERE event_timestamp IS NULL
                               OR created IS NULL
                        ) AS missing_timestamp_rows,

                        COUNT(*) FILTER (
                            WHERE violation_rate_7d < 0
                               OR violation_rate_7d > 1
                        ) AS invalid_rate_rows,

                        COUNT(*) FILTER (
                            WHERE action_count <= 0
                               OR violation_count < 0
                               OR violation_count > action_count
                        ) AS invalid_count_rows,

                        MIN(violation_rate_7d) AS minimum_rate,
                        MAX(violation_rate_7d) AS maximum_rate,
                        MIN(event_timestamp) AS minimum_event_timestamp,
                        MAX(event_timestamp) AS maximum_event_timestamp
                    FROM {FEATURE_TABLE}
                    """
                )

                (
                    feature_rows,
                    distinct_agents,
                    distinct_event_timestamps,
                    missing_timestamp_rows,
                    invalid_rate_rows,
                    invalid_count_rows,
                    minimum_rate,
                    maximum_rate,
                    minimum_event_timestamp,
                    maximum_event_timestamp,
                ) = cursor.fetchone()

                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT agent_id
                        FROM {FEATURE_TABLE}
                        GROUP BY agent_id
                        HAVING COUNT(*) > 1
                    ) duplicate_agents
                    """
                )
                duplicate_agent_count = cursor.fetchone()[0]

                # Recompute the number of source agents in the same
                # seven-day interval to ensure no agent was lost.
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT agent.agent_id)
                    FROM fact_agent_action AS fact
                    JOIN dim_agent AS agent
                      ON fact.agent_key = agent.agent_key
                    WHERE fact.called_at > %s
                      AND fact.called_at <= %s
                    """,
                    (
                        expected["window_start"],
                        expected["event_timestamp"],
                    ),
                )
                expected_agent_count = cursor.fetchone()[0]

        finally:
            connection.close()

        assert feature_rows == expected["feature_rows"], (
            f"Feature count mismatch: "
            f"{feature_rows} != {expected['feature_rows']}"
        )

        assert feature_rows == expected_agent_count, (
            f"Feature table has {feature_rows} agents but source "
            f"window has {expected_agent_count}"
        )

        assert distinct_agents == feature_rows, (
            "agent_id is not unique in the feature table"
        )

        assert duplicate_agent_count == 0, (
            f"Found {duplicate_agent_count} duplicated agents"
        )

        assert distinct_event_timestamps == 1, (
            "Feature rows do not share one snapshot event_timestamp"
        )

        assert missing_timestamp_rows == 0, (
            f"Found {missing_timestamp_rows} rows missing "
            "event_timestamp or created"
        )

        assert invalid_rate_rows == 0, (
            f"Found {invalid_rate_rows} feature rates outside [0, 1]"
        )

        assert invalid_count_rows == 0, (
            f"Found {invalid_count_rows} invalid count combinations"
        )

        assert minimum_event_timestamp == maximum_event_timestamp, (
            "Feature snapshot contains multiple event timestamps"
        )

        assert minimum_rate is not None and maximum_rate is not None
        assert 0 <= float(minimum_rate) <= 1
        assert 0 <= float(maximum_rate) <= 1

        print(
            "[dp3 validation] PASS — "
            f"feature_rows={feature_rows}, "
            f"distinct_agents={distinct_agents}, "
            f"event_timestamp={maximum_event_timestamp.isoformat()}, "
            f"minimum_rate={float(minimum_rate):.4%}, "
            f"maximum_rate={float(maximum_rate):.4%}, "
            "missing_timestamps=0, duplicates=0, invalid_rates=0"
        )

    feature_summary = compute_load_offline_feature()
    validate_feature(feature_summary)


dp3_offline_feature()
