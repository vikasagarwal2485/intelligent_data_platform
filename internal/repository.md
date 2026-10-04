# Repository map

Reviewed: 2026-10-04 against baseline commit `0be5efe` and the workspace setup
changes. Recheck the implementation when making a change.

IDP is a multi-tenant telecom lakehouse for alternative credit-limit scoring and
fraud detection. The batch path runs Spark locally; the cluster path uses Kafka,
Spark Operator and Airflow with the same stage code.

| Area | Source and behavior |
|---|---|
| Package and CLI | [src/idp/](../src/idp/) and [cli.py](../src/idp/cli.py); installed as `idp` |
| Stages | [pipeline.py](../src/idp/pipeline.py): Bronze, Silver, Gold features, labels, train, score, publish |
| Raw ingestion | [ingestion/raw.py](../src/idp/ingestion/raw.py), [streaming/](../src/idp/streaming/); raw TMF events into append-only Bronze Iceberg tables |
| Source contracts | [config/tmf/](../config/tmf/) schemas, extensions and profiles; [data_samples/](../data_samples/) synthetic TMF examples |
| Silver | [transforms/conform.py](../src/idp/transforms/conform.py), [silver.py](../src/idp/transforms/silver.py), [incremental.py](../src/idp/transforms/incremental.py); conform, latest dimensions, incremental facts, quarantine |
| Gold features | [transforms/features.py](../src/idp/transforms/features.py); shared aggregates, consenting subscribers only |
| Canonical contracts | [config/avro/](../config/avro/); canonical records and Silver table schemas |
| Lake/catalog | [common/catalog.py](../src/idp/common/catalog.py), [transforms/lake.py](../src/idp/transforms/lake.py); SQL/JDBC catalog shared by pyiceberg and Spark |
| ML | [ml/](../src/idp/ml/), [transforms/sampling.py](../src/idp/transforms/sampling.py); bounded training sample, credit/fraud models, MLflow champion or object-store fallback |
| Inference | [transforms/scoring.py](../src/idp/transforms/scoring.py); distributed scoring, append-only Gold score history |
| Serving | [serving/](../src/idp/serving/); Spark JDBC publish and atomic swap into PostgreSQL/TimescaleDB, FastAPI scoring, Keycloak JWT tenant checks |
| Analytics | [mcp/](../src/idp/mcp/); seven tenant-scoped, read-only tools over Trino Silver and Gold |
| Runtime settings | [config/platform.yaml](../config/platform.yaml), [common/config.py](../src/idp/common/config.py); explicit `IDP_*` overrides |
| Shared services | [deploy/platform/](../deploy/platform/); namespace `idp-platform` |
| Workloads | [deploy/app/](../deploy/app/); namespace `idp-app`, app overlays, non-Spark Jobs and generated SparkApplications |
| Orchestration | [idp_medallion_dag.py](../pipelines/airflow/dags/idp_medallion_dag.py); four DAGs listed below |
| Verification | [tests/](../tests/), [CI](../.github/workflows/ci.yml), [workspace checker](../scripts/check_workspace.py) |

The lake has `bronze`, `silver` and `gold` Iceberg namespaces. Local catalog state
is SQLite under `storage.root`; the cluster uses PostgreSQL's `iceberg` database
with catalog name `lake`. Object storage uses one bucket per layer, plus landing
and ML buckets. Nessie is provisioned but is not the lake's active catalog.

Silver tables include `dim_subscriber`, `dim_customer`, `dim_billing_account`,
`dim_product`, `fact_usage`, `fact_recharge`, `fact_fee_charge` and `quarantine`.
Gold holds `credit_features`, `fraud_features`, `credit_scores` and `fraud_scores`.
Labels are a separate store; serving tables are a materialized copy of Gold.

Airflow runs `idp_features` (hourly Silver → Gold), `idp_training` (daily train),
`idp_inference` (score → publish on new features OR a model), and
`idp_maintenance` (daily maintain). Labels must be seeded separately. All DAG
tasks submit the same SparkApplications from `deploy/app/spark/`.

The package is currently `0.1.0`. At baseline inspection there were no release
tags or active OpenSpec changes. Phases 0–15 and subsequent changes are archived;
the [roadmap](../docs/roadmap.md) is historical, not a queue of unimplemented work.
