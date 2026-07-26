# Architecture

## Diagram
![Pipeline Architecture](Pipeline.svg)

## Main Components (each block is a deployable unit)

### Batch Data Pipeline • DP1 → DP2 → DP3
Offline Event Generator (agents, tools, agent_action_history) writes Parquet
to MinIO Source Landing (**B1**). Bronze Raw Data • DP1 reads and
canonicalizes it (**B2**), producing `raw_*` tables. Apache Spark • DP2
reads Bronze (**B3**), processes it, and writes Silver Clean Data (**B4**,
`stg_*` tables). The Gold Model (`dim_agent`, `dim_tool`, `fact_agent_action`)
is built from Silver in PostgreSQL Gold Warehouse (**B5**), then Offline
Feature • DP3 computes `feat_agent_guardrail_violation_rate_7d` (**B6**).

### Streaming Pipeline • Flink Standalone
Real-time Event Generator publishes JSON `agent_events` (**S1**) to Apache
Kafka • KRaft. Apache Flink consumes the topic (**S2**), processes the
stream, and outputs to console/log only — **not connected to the Gold
Model**.

### Interactive Telemetry Path (Novel Idea 1)
AI Agent Runtime • IDE • CLI Assistant (Codex in VS Code) exports OTel
telemetry (**T1**) to the OpenTelemetry Collector, which writes JSONL to
MinIO Telemetry Landing (**T2**). Bronze Raw Data • DP1 also reads this
telemetry and canonicalizes it alongside synthetic data (**T3**).

### Orchestration • Governance • Data Access
Apache Airflow orchestrates DP1, DP2, and DP3 (**O1**–**O3**: run
ingestion / batch transform / feature job) and runs Great Expectations
validation tasks, then publishes lineage, quality, and contract information
to DataHub (**O4**). Hive Metastore catalogs lake tables (**D1**), Trino
queries them via SQL (**D3**) and also reads Parquet directly from MinIO
(**D2**). DBeaver connects to both Trino and PostgreSQL to query Gold data
and view the ERD (**D4**).

## Arrow Convention
Each arrow follows the actual direction of data flow, carries a short label,
and is numbered per flow: **B** = Batch (B1–B6), **S** = Streaming (S1–S2),
**T** = Telemetry (T1–T3), **O** = Orchestration (O1–O4), **D** = Data
Access (D1–D4).