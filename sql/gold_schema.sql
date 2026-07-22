-- Gold Warehouse schema for Day 7.
-- dim_agent uses SCD Type 2.
-- Foreign keys are real constraints so DBeaver can generate an ERD.

DROP TABLE IF EXISTS fact_agent_action;
DROP TABLE IF EXISTS dim_agent;
DROP TABLE IF EXISTS dim_tool;

CREATE TABLE dim_agent (
    agent_key       TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    team            TEXT NOT NULL,
    risk_level      TEXT NOT NULL,
    model_version   TEXT NOT NULL,
    valid_from_ts   TIMESTAMPTZ NOT NULL,
    valid_to_ts     TIMESTAMPTZ,
    is_current      BOOLEAN NOT NULL
);

CREATE TABLE dim_tool (
    tool_id         TEXT PRIMARY KEY,
    tool_name       TEXT NOT NULL,
    category        TEXT NOT NULL
);

CREATE TABLE fact_agent_action (
    action_id                  TEXT PRIMARY KEY,
    run_id                     TEXT NOT NULL,
    agent_key                  TEXT NOT NULL
        REFERENCES dim_agent(agent_key),
    tool_id                    TEXT NOT NULL
        REFERENCES dim_tool(tool_id),
    called_at                  TIMESTAMPTZ NOT NULL,
    duration_ms                INTEGER NOT NULL,
    tokens_used                INTEGER,
    guardrail_policy_version   TEXT,
    is_violation               BOOLEAN NOT NULL
);
