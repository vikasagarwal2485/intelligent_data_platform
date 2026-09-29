"""Airflow DAGs that orchestrate the medallion stages (production mode).

Airflow is the ORCHESTRATOR (schedule / order / retries); every stage runs from
the one `idp` image, the same code as the CLI (REQ-ORC-05):
  * the Spark stages (silver, gold, score, publish, maintain) are submitted as Spark
    Operator SparkApplications (REQ-PLT-10): a driver pod plus executor pods, sized by
    `spark.profiles.<stage>`. The manifests are rendered by `make spark-manifests`
    into ./spark/ next to this file (baked into the DAG image). A task succeeds only
    when its application completes; a failed application fails the task.
  * labels and train run as single pods (train uses a local Spark session for its
    bounded, stratified sample, REQ-ML-09).
There is no engine switch.

Publish is a SEPARATE, independent task (Gold -> TimescaleDB) — never folded into
Gold. Next step: migrate its target from TimescaleDB to Iceberg tables via the
Nessie catalog (the serving read path changes, the task stays independent).

This is the correct decoupling: Airflow only needs permission to launch pods — it
does NOT need the `idp` package or Spark installed. The pods carry code + deps.

Prerequisites:
  * `idp/scoring-api:dev` on the node (`make image`; bundles the JRE, pyspark, the
    S3A jars and the Iceberg jars)
  * the lake's Iceberg catalog database (`iceberg` on TimescaleDB, `make lake-catalog`)
  * the Spark Operator watching idp-app (`make spark-operator`)
  * the Airflow SA may create pods and SparkApplications in `idp-app`
    (deploy/platform/airflow/rbac.yaml)

DAGs (four, decoupled by cadence; the first three wired via Airflow Datasets):
  * idp_features  : silver >> gold           (hourly; PRODUCES gold-features dataset)
  * idp_inference : score  >> publish        (DATASET-DRIVEN: runs on new features
                    OR a new model; loads the current model each run)
  * idp_training  : labels >> train          (daily; PRODUCES model dataset)
  * idp_maintenance: maintain                (daily; compaction, snapshot expiry,
                    orphan cleanup -- produces no dataset, so it triggers nothing)

So a new model auto-re-scores (inference fires on the model dataset) and fresh
features auto-re-score — no manual second pass. Requires Airflow >= 2.9 for the
`|` (OR) dataset condition.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from kubernetes.client import models as k8s

IMAGE = "idp/scoring-api:dev"
APP_NS = "idp-app"
BUCKET_PREFIX = "idp"
MINIO_ENDPOINT = "http://minio.idp-platform.svc:9000"
# The lake's Iceberg JDBC catalog: database `iceberg` on the platform Postgres.
LAKE_CATALOG_URI = "postgresql+psycopg2://postgres:idp-dev-password@timescaledb.idp-platform.svc:5432/iceberg"

# Datasets wire the three DAGs together (Airflow data-aware scheduling). The
# `gold` task produces GOLD_FEATURES; the `train` task produces MODEL; the
# inference DAG is scheduled on (GOLD_FEATURES | MODEL) so it runs on either a
# feature refresh OR a new model — no manual re-trigger. (URIs are logical
# identifiers, not fetched by Airflow.)
GOLD_FEATURES = Dataset("s3://idp-gold/credit_features")
MODEL = Dataset("s3://idp-ml/credit_model.joblib")


# S3/MinIO env every stage pod needs (platform services are in idp-platform).
S3_ENV = [
    k8s.V1EnvVar(name="IDP_S3_ENDPOINT", value=MINIO_ENDPOINT),
    k8s.V1EnvVar(name="IDP_S3_ACCESS_KEY", value="minioadmin"),
    k8s.V1EnvVar(name="IDP_S3_SECRET_KEY", value="minioadmin123"),
    k8s.V1EnvVar(name="IDP_S3_BUCKET_PREFIX", value=BUCKET_PREFIX),
]
# The lake's Iceberg catalog (REQ-FND-04), shared by pyiceberg and Spark.
CATALOG_ENV = [
    k8s.V1EnvVar(name="IDP_CATALOG_TYPE", value="sql"),
    k8s.V1EnvVar(name="IDP_CATALOG_URI", value=LAKE_CATALOG_URI),
]
LAKE_ENV = S3_ENV + CATALOG_ENV
# Synthetic-generation volume for the `labels` task. MUST MATCH the produce job
# (deploy/app/jobs/produce-job.yaml) — labels are regenerated deterministically
# from these params and must line up with the data that became Gold, or the
# training join is empty. Keep in lock-step with produce-job.yaml.
SYN_ENV = [
    k8s.V1EnvVar(name="IDP_SUBSCRIBERS", value="150"),
    k8s.V1EnvVar(name="IDP_DAYS", value="14"),
]
# train: a local Spark session (train profile: 2g heap + 1g overhead) for the
# bounded training sample, then scikit-learn on that sample.
TRAIN_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1536Mi"},
    limits={"cpu": "2", "memory": "3Gi"},
)

default_args = {"owner": "idp", "retries": 1, "retry_delay": timedelta(minutes=3)}


def _stage(task_id: str, arg: str, *, extra_env=None, resources=None) -> KubernetesPodOperator:
    """A medallion stage: `idp <arg>` in the idp image (Silver/Gold run Spark inside it)."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"idp-{arg}",
        namespace=APP_NS,
        image=IMAGE,
        image_pull_policy="Never",          # local image built into minikube
        cmds=["idp"],
        arguments=[arg],
        env_vars=LAKE_ENV + (extra_env or []),
        container_resources=resources,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",  # keep FAILED stage pods in idp-app for debugging
        in_cluster=True,
    )


def _spark_stage(task_id: str, stage: str) -> SparkKubernetesOperator:
    """A Spark stage as a SparkApplication (driver + executors) in idp-app, from the
    rendered manifest ./spark/<stage>.yaml. The task follows the driver and fails if
    the application fails (SCN-ORC-02.6); the application is deleted afterwards."""
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=APP_NS,
        application_file=f"spark/{stage}.yaml",
        kubernetes_conn_id="kubernetes_default",
        in_cluster=True,
        get_logs=True,
        delete_on_termination=True,
    )


# DAG 1 — FEATURES (refresh): Silver -> Gold (features only), on the data cadence.
# The gold task PRODUCES the `gold-features` dataset, which drives inference.
with DAG(
    dag_id="idp_features",
    description="Feature refresh: Silver -> Gold (features only)",
    schedule="0 * * * *",                   # hourly (data cadence)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One feature refresh at a time: two Silver runs would share the streaming
    # checkpoints (Structured Streaming allows one active query per checkpoint),
    # and overlapping runs stack several Spark pods on the node.
    max_active_runs=1,
    default_args=default_args,
    tags=["idp", "medallion", "features"],
) as dag_features:
    _gold = _spark_stage("gold", "gold")
    _gold.outlets = [GOLD_FEATURES]          # updating features triggers inference
    _spark_stage("silver", "silver") >> _gold


# DAG 2 — INFERENCE (serve): Score -> Publish. DATASET-DRIVEN — it runs whenever
# NEW features land OR a NEW model is promoted (Airflow >= 2.9 `|` = OR). It always
# loads the current model from the store, so a fresh model auto-re-scores with no
# manual re-run; graceful (no scores) until a model exists.
with DAG(
    dag_id="idp_inference",
    description="Inference + serve: Score -> Publish (runs on new features OR new model)",
    schedule=(GOLD_FEATURES | MODEL),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["idp", "ml", "inference"],
) as dag_inference:
    _spark_stage("score", "score") >> _spark_stage("publish", "publish")


# DAG 3 — TRAINING: labels -> train, on its own (daily) cadence. The train task
# PRODUCES the `model` dataset (which triggers inference). The `labels` task seeds
# the outcomes store — in this prototype it stands in for the business's real
# observed-outcomes feed; in production replace it with that feed and keep `train`.
with DAG(
    dag_id="idp_training",
    description="Offline ML training on Gold features + labels store",
    schedule="0 2 * * *",                   # daily 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["idp", "ml", "training"],
) as dag_training:
    _train = _stage("train", "train", resources=TRAIN_RESOURCES)
    _train.outlets = [MODEL]                 # a new model triggers inference
    _stage("labels", "labels", extra_env=SYN_ENV) >> _train


# DAG 4 — MAINTENANCE: compaction, snapshot expiry and orphan cleanup on every lake
# table (REQ-ORC-06), daily in a quiet hour. It declares no outlets, so it never
# triggers the features or inference DAGs; Iceberg's optimistic commits keep it
# safe next to a concurrent Silver/Gold run (a conflicting commit is retried).
with DAG(
    dag_id="idp_maintenance",
    description="Lake maintenance: compact, expire snapshots, remove orphan files",
    schedule="0 3 * * *",                   # daily 03:00, after training
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["idp", "lake", "maintenance"],
) as dag_maintenance:
    _spark_stage("maintain", "maintain")
