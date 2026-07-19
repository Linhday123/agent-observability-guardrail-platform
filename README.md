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

## Assumptions & Scope
- Data is controlled synthetic data, except for the Interactive Telemetry
  Path, which uses real data from Codex.
- The Streaming Pipeline stops at Apache Flink and is not connected to the
  Gold Model.

## Repo Structure
```
src/generator/     — Offline Event Generator 
infra/otel/         — OpenTelemetry Collector config 
docs/architecture/  — Architecture diagram + explanation
docs/engineering/   — Docker / Docker Compose optimization
docs/evidence/      — Screenshot evidence, organized by topic
```

## Documentation Index
- [Architecture](docs/architecture/architecture.md)
- [Docker Optimization](docs/engineering/docker-optimization.md)

## Run Instructions
```bash
cp .env.example .env
docker compose up -d
docker compose ps
```