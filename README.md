# Agent Observability & Guardrail Monitoring Platform

## Table of Contents
1. [Domain Overview](#domain-overview)
2. [Business Problem](#business-problem)
3. [Stakeholders](#stakeholders)
4. [Architecture](#architecture)
5. [Glossary](#glossary)
6. [Assumptions & Scope](#assumptions--scope)
7. [Repo Structure](#repo-structure)
8. [Documentation Index](#documentation-index)
9. [Run Instructions](#run-instructions)

## Domain Overview
A data platform for monitoring AI agents (Agent Observability) and guardrail
violations (Guardrail Monitoring) across an organization's automated AI
agents.

## Business Problem
"What are the AI agents running in my organization doing, and are any of
them doing something wrong?"

## Stakeholders
- Platform/Data Engineering team — operates the pipeline
- AI Safety team — monitors the guardrail violation rate
- Engineering leads — manage agents by team and risk level

## Architecture
![Pipeline Architecture](docs/architecture/Pipeline.svg)

See the full breakdown in [`docs/architecture/architecture.md`](docs/architecture/architecture.md).

## Glossary
| Term | Meaning |
|---|---|
| DP1 | Ingestion — Source/Telemetry Landing → Bronze Raw Data |
| DP2 | Batch processing — Bronze → Silver → Gold Model |
| DP3 | Offline feature — Gold Model → Offline Feature |
| Guardrail | Rules an agent must not violate |
| SCD2 | Slowly Changing Dimension Type 2 — tracks the full history of a
dimension's changes over time, instead of overwriting it |
| Gold Model | The dimensional warehouse (`dim_agent`, `dim_tool`,
`fact_agent_action`) built in PostgreSQL |

## Assumptions & Scope
- Data is controlled synthetic data, except for the Interactive Telemetry
  Path, which uses real data from Codex.
- The Streaming Pipeline stops at Apache Flink and is not connected to the
  Gold Model.
- `fact_agent_action.is_violation` is a deterministic, reproducible flag
  derived from `action_id`, since the underlying source data has no
  native guardrail-violation signal. This is a documented coursework
  design assumption.

## Repo Structure

src/generator/ — Offline Event Generator
src/streaming_generator/ — Real-time Event Generator (Kafka producer)
src/streaming_processing/ — Flink baseline + optimized jobs
src/batch_processing/ — Spark baseline + optimized jobs
src/storage_optimization/ — Lakehouse compaction script
infra/otel/ — OpenTelemetry Collector config
infra/airflow/ — Airflow custom image + compose
infra/flink/ — Flink Standalone cluster (JobManager/TaskManager)
dags/ — Airflow DAGs (DP1, DP2, DP3)
sql/ — Gold warehouse schema + indexes
docs/architecture/ — Architecture diagram + explanation
docs/engineering/ — Docker / Docker Compose optimization
docs/generator/ — Offline + streaming data quality
docs/spark/, docs/flink/ — Batch/stream optimization reports
docs/storage/ — Lakehouse + warehouse optimization
docs/orchestration/ — Airflow pipeline documentation
docs/novel-ideas/ — Optional novel idea write-ups
docs/evidence/ — Screenshot evidence, organized by topic


## Documentation Index
- [Architecture](docs/architecture/architecture.md)
- [Docker Optimization](docs/engineering/docker-optimization.md)
- [Offline Data Quality](docs/generator/offline-data-quality.md)
- [Streaming Data Quality](docs/generator/streaming-data-quality.md)
- [Interactive Telemetry Path (Novel Idea)](docs/novel-ideas/opentelemetry.md)
- [Airflow Pipelines](docs/orchestration/airflow-pipelines.md)
- [Spark Optimization](docs/spark/optimization-report.md)
- [Flink Optimization](docs/flink/optimization-report.md)
- [Storage Optimization](docs/storage/storage-optimization.md)

## Run Instructions
```bash
cp .env.example .env
docker network create data-stack-network
docker compose up -d
docker compose -f infra/airflow/docker-compose.yml --env-file .env up -d
docker compose -f infra/flink/docker-compose.yml up -d
docker compose ps
```
