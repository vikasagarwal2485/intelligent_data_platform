"""Airflow DAGs that orchestrate the medallion stages (production mode).

Airflow is the ORCHESTRATOR (schedule / order / retries); every stage runs from
the one `idp` image, the same code as the CLI (REQ-ORC-05). Every task is a Spark
Operator SparkApplication (REQ-ORC-02, REQ-PLT-10): a driver pod plus executor pods,
sized by `spark.profiles.<stage>`, for silver, gold, score, publish, maintain and
train. The manifests are rendered by `make spark-manifests` into deploy/app/spark/
(their one copy in the repo), and the DAG image bakes them in as ./spark/ next to
this file (a named build context). A task succeeds only when its application
completes; a failed application fails the task. No task launches a pod of its own,
and there is no engine switch.

The non-Spark stages (generate, produce, consume, labels) are CLI stages run by
`make` as Kubernetes Jobs, outside Airflow. In particular the labels store is seeded
with `make labels` (it stands in for the business's observed-outcomes feed); the
training DAG only trains, and fails with a "seed labels" message if it is empty.

Publish is a SEPARATE, independent task (Gold -> TimescaleDB) — never folded into
Gold.

This is the correct decoupling: Airflow only needs permission to submit
SparkApplications (and read their pods' logs) — it does NOT need the `idp` package
or Spark installed. The pods carry code + deps.

Prerequisites:
  * `idp/scoring-api:dev` on the node (`make image`; bundles the JRE, pyspark, the
    S3A jars and the Iceberg jars)
  * the lake's Iceberg catalog database (`iceberg` on TimescaleDB, `make lake-catalog`)
  * the Spark Operator watching idp-app (`make spark-operator`)
  * the Airflow SA may submit SparkApplications in `idp-app`
    (deploy/platform/airflow/rbac.yaml)
  * a seeded labels store for training (`make labels`)

DAGs (four, decoupled by cadence; the first three wired via Airflow Datasets):
  * idp_features  : silver >> gold           (hourly; PRODUCES gold-features dataset)
  * idp_inference : score  >> publish        (DATASET-DRIVEN: runs on new features
                    OR a new model; loads the current model each run)
  * idp_training  : train                    (daily; PRODUCES model dataset)
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
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

APP_NS = "idp-app"

# Datasets wire the three DAGs together (Airflow data-aware scheduling). The
# `gold` task produces GOLD_FEATURES; the `train` task produces MODEL; the
# inference DAG is scheduled on (GOLD_FEATURES | MODEL) so it runs on either a
# feature refresh OR a new model — no manual re-trigger. (URIs are logical
# identifiers, not fetched by Airflow.)
GOLD_FEATURES = Dataset("s3://idp-gold/credit_features")
MODEL = Dataset("s3://idp-ml/credit_model.joblib")


default_args = {"owner": "idp", "retries": 1, "retry_delay": timedelta(minutes=3)}


def _spark_stage(task_id: str, stage: str) -> SparkKubernetesOperator:
    """A Spark stage as a SparkApplication (driver + executors) in idp-app, from the
    rendered manifest ./spark/<stage>.yaml (deploy/app/spark/ in the repo, baked into
    the DAG image). The task follows the driver and fails if
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


# DAG 3 — TRAINING: train, on its own (daily) cadence, as a SparkApplication (the
# join and the stratified sample on the executors, fitting on the driver). It
# PRODUCES the `model` dataset (which triggers inference). The labels store it reads
# is seeded outside Airflow (`make labels`, a stand-in for the business's observed-
# outcomes feed); with no labels, train fails and the model dataset isn't updated.
with DAG(
    dag_id="idp_training",
    description="Offline ML training on Gold features + labels store",
    schedule="0 2 * * *",                   # daily 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["idp", "ml", "training"],
) as dag_training:
    _train = _spark_stage("train", "train")
    _train.outlets = [MODEL]                 # a new model triggers inference


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
