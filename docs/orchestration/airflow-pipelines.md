# Airflow Pipelines

## Deployment
Airflow 3.0.0 (LocalExecutor) is deployed via `infra/airflow/docker-compose.yml`,
separate from the root compose file, sharing the `data-stack-network`
external network so it can reach `minio` by service name. It uses its own
metadata database (`postgres-airflow`), independent from `postgres-gold`
(Gold Warehouse). Airflow 3.x splits DAG parsing into a dedicated
`airflow-dag-processor` service and renames the webserver to
`airflow-api-server`; `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` must point
to the api-server's service name, or task execution silently fails even
though the UI loads fine. `AIRFLOW__API_AUTH__JWT_SECRET` must also be set
explicitly and shared across every Airflow component, otherwise each
process signs/validates tokens with its own randomly generated secret and
every task execution is rejected with an auth error even though the DAG
parses and triggers correctly. No `airflow-triggerer` service is included,
since this DAG uses only regular TaskFlow tasks, no deferrable operators.
This deployment intentionally disables Fernet encryption
(`AIRFLOW__CORE__FERNET_KEY: ''`) since it is a local environment with no
production secrets; this is unrelated to the JWT secret, which authenticates
communication between Airflow components rather than encrypting stored
credentials. The custom image (`infra/airflow/Dockerfile`) is based on
`apache/airflow:3.0.0-python3.11` with `apache-airflow-providers-fab`
(required for CLI user management on Airflow 3.x) and
`great_expectations==0.18.19`, installed in a separate `RUN` step from
Airflow's own dependencies to avoid a `ruamel.yaml` version conflict
between the two packages' constraints.

## dp1_bronze_ingest
Two tasks: `ingest_minio_to_bronze` copies every Parquet file from
`Source Landing` into `Bronze Raw Data`, preserving the old/recent
partition split so schema evolution remains visible to batch processing.
`validate_bronze` then runs a Great Expectations suite (`ge.from_pandas()`
fluent API) on each Bronze table, checking row counts, required columns,
null constraints, and uniqueness where appropriate. The boto3 client
forces path-style S3 addressing, which is required for reliable
connectivity to MinIO over the Docker network.

## Proof it worked
![DP1 DAG run](../evidence/dp1-ingestion/dp1_dag_success.png)
![Bronze files in MinIO](../evidence/dp1-ingestion/minio_bronze_files.png)

Triggering `dp1_bronze_ingest` from the Airflow UI completed both tasks
successfully, in the correct order, confirming Bronze Raw Data now
contains `raw_agents`, `raw_tools`, and `raw_agent_actions` (split into
old/recent partitions) derived from Source Landing, validated by Great
Expectations.

## dp2_silver_gold

`dp2_silver_gold` refreshes Silver Clean Data with Spark Local and then
loads the dimensional Gold model into PostgreSQL.

The DAG contains two ordered stages:

```text
spark_bronze_to_silver_gold
              ↓
validate_silver_gold
```

![DP2 successful DAG run](../evidence/dp2-dp3-pipeline/dp2_airflow_graph_success.png)

### Stage 1 — `spark_bronze_to_silver_gold`

The first task invokes the optimized Spark job:

```text
src/batch_processing/batch_optimized.py
```

Spark runs inside the Airflow container with:

```python
.master("local[*]")
```

The Spark stage refreshes Bronze to Silver using the following
transformations:

- `mergeSchema` combines the old and recent action schemas.
- A window operation deduplicates records by `action_id`.
- Deterministic manual salting distributes the skewed `tool_id`
  aggregation.
- `bucketBy(8, "action_id")` avoids high-cardinality partition
  directories.
- Silver agent, tool, and action datasets are written to MinIO.

The Spark result was:

```text
Input action rows:                   40,800
Rows after action_id deduplication:  40,000
Silver action data files:                 8
Silver action rows:                  40,000
```

After Spark completes, the task reads:

```text
silver/stg_agents.parquet/
silver/stg_tools.parquet/
silver/stg_agent_actions/
```

It then builds and loads the following Gold tables:

```text
dim_agent
dim_tool
fact_agent_action
```

### Point-in-Time SCD2 Resolution

Silver actions contain the business key `agent_id`, while
`fact_agent_action` requires the SCD2 surrogate key `agent_key`.

Each action is matched to the agent version satisfying:

```text
valid_from_ts <= called_at < valid_to_ts
```

For the current version, a null `valid_to_ts` represents an open-ended
validity interval.

Because the action history and dimension history were generated
independently, 826 actions initially occurred before their agents'
earliest recorded SCD2 versions.

DP2 backdates the earliest SCD2 version for six affected agents to
their earliest observed action timestamp. This alignment is applied
only to the Gold dimension load. Bronze and Silver remain unchanged.

### Derived Guardrail Violation Flag

The generated source data does not contain a native guardrail violation
flag. DP2 therefore derives `is_violation` as an explicit coursework
design assumption.

The implementation applies SHA-256 to `action_id` and uses a stable
threshold designed to produce approximately an 8% violation rate:

```text
first 8 SHA-256 hexadecimal digits
              ↓
convert to integer
              ↓
value modulo 100 < 8
```

Unlike Python's built-in `hash()`, SHA-256 produces the same result
across processes and repeated DAG runs.

![DP2 ingest and Gold load log](../evidence/dp2-dp3-pipeline/dp2_ingest_task_log.png)

The Gold load result was:

| Gold table | Rows |
|---|---:|
| `dim_agent` | 67 |
| `dim_tool` | 12 |
| `fact_agent_action` | 40,000 |

The generated overall fact-level violation rate was approximately
8.11%.

![Gold Warehouse row counts](../evidence/dp2-dp3-pipeline/dp2_gold_row_counts.png)

### Stage 2 — `validate_silver_gold`

The validation task checks that:

- Gold row counts match the load summary.
- `agent_key` has no orphaned foreign-key references.
- `tool_id` has no orphaned foreign-key references.
- Every action belongs to the validity interval of its assigned SCD2
  agent version.
- Every business `agent_id` has exactly one current version.
- The generated violation rate remains within the expected coursework
  range.

![DP2 validation log](../evidence/dp2-dp3-pipeline/dp2_validate_task_log.png)

Validation result:

```text
[dp2 validation] PASS
dim_agent=67
dim_tool=12
fact_agent_action=40000
violation_rate=8.1125%
orphans=0
invalid_scd2_mappings=0
```

## dp3_offline_feature

`dp3_offline_feature` materializes and validates the offline guardrail
feature:

```text
feat_agent_guardrail_violation_rate_7d
```

The DAG contains two ordered stages:

```text
compute_load_offline_feature
              ↓
validate_feature
```

![DP3 successful DAG run](../evidence/dp2-dp3-pipeline/dp3_airflow_graph_success.png)

### Stage 1 — `compute_load_offline_feature`

DP3 reads the PostgreSQL Gold tables rather than running Spark again:

```text
fact_agent_action
        +
dim_agent
        ↓
seven-day aggregation by agent_id
```

The latest action timestamp in Gold is used as the reproducible feature
observation time:

```sql
MAX(fact_agent_action.called_at)
```

This value becomes `event_timestamp`.

The feature window is:

```text
event_timestamp - 7 days
              →
event_timestamp
```

Anchoring the computation to the latest event in the dataset, rather
than the current server clock, makes repeated coursework runs
reproducible.

For each `agent_id`, DP3 calculates:

```text
violation_rate_7d
=
violation_count / action_count
```

The materialized table contains:

```text
agent_id
violation_count
action_count
violation_rate_7d
event_timestamp
created
```

Where:

- `event_timestamp` is the observation time of the feature snapshot.
- `created` is the PostgreSQL warehouse load timestamp.

The compute result was:

```text
Feature rows:             50
Actions in 7-day window:  2,896
Violations in window:       203
```

The unweighted mean of the 50 per-agent violation rates was
approximately 6.96%.

![DP3 compute and load log](../evidence/dp2-dp3-pipeline/dp3_compute_task_log.png)

### Stage 2 — `validate_feature`

The validation task checks that:

- One feature row exists for every agent represented in the source
  seven-day window.
- `agent_id` is unique.
- `event_timestamp` and `created` are not null.
- All rows share one snapshot `event_timestamp`.
- `action_count` is greater than zero.
- `violation_count` is between zero and `action_count`.
- `violation_rate_7d` is between zero and one.
- No duplicate feature rows exist.

![DP3 validation log](../evidence/dp2-dp3-pipeline/dp3_validate_task_log.png)

Validation result:

```text
[dp3 validation] PASS
feature_rows=50
distinct_agents=50
minimum_rate=1.7857%
maximum_rate=13.7255%
missing_timestamps=0
duplicates=0
invalid_rates=0
```

The minimum and maximum rates represent individual agent feature
values. The reported 6.96% compute-stage value is the unweighted mean
of all per-agent rates.

### Materialized Feature Sample

![Offline feature sample](../evidence/dp2-dp3-pipeline/dp3_feature_sample.png)

The sample confirms that the feature table contains business keys,
counts, the seven-day violation rate, and both required time columns:

```text
event_timestamp
created
```
