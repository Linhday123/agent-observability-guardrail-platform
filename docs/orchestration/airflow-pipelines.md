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
