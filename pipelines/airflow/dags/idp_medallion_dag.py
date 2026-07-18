"""Airflow DAGs that orchestrate the medallion stages (production mode).

Airflow is the ORCHESTRATOR (schedule / order / retries); each stage runs as a
POD it launches. The Gold stage can run on either EXECUTION ENGINE, selected by a
switch — the DAG shape (silver >> gold >> publish) is identical either way:

  * pandas (default) — launches `idp gold` in the scoring-api image (single node).
  * spark            — launches `spark-submit pipelines/spark/gold_job.py` in an
                       apache/spark pod (distributed; for large data / backfills).

Pick the engine with the Airflow Variable `idp_gold_engine` (`pandas` | `spark`),
or the env var `IDP_GOLD_ENGINE`; default `pandas`. The spark path mounts the job
script from the `idp-gold-spark` ConfigMap (create it with `make spark-configmap`).

This is the correct decoupling: Airflow only needs permission to launch pods — it
does NOT need the `idp` package or Spark installed. The pods carry code + deps.

Prerequisites:
  * the `idp/scoring-api:dev` image is on the node (`make image`)
  * for spark gold: the `idp-gold-spark` ConfigMap exists in idp-app (`make spark-configmap`)
  * the Airflow service account may create pods in `idp-app`
    (apply deploy/platform/airflow/rbac.yaml; see docs/triggering.md)

DAGs:
  * idp_silver_gold : silver >> gold >> publish   (hourly)
  * idp_train       : train                       (daily)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = "idp/scoring-api:dev"
SPARK_IMAGE = "apache/spark:3.5.3"
APP_NS = "idp-app"
BUCKET_PREFIX = "idp"
MINIO_ENDPOINT = "http://minio.idp-platform.svc:9000"

# --- Gold execution engine switch -------------------------------------------
# Airflow Variable `idp_gold_engine` wins (settable in the UI without redeploy);
# falls back to env IDP_GOLD_ENGINE, then to "pandas". Wrapped so a metadata-DB
# hiccup at parse time can't break DAG import.
def _gold_engine() -> str:
    try:
        from airflow.models import Variable
        val = Variable.get("idp_gold_engine",
                           default_var=os.environ.get("IDP_GOLD_ENGINE", "pandas"))
    except Exception:
        val = os.environ.get("IDP_GOLD_ENGINE", "pandas")
    return (val or "pandas").strip().lower()


# S3/MinIO env every stage pod needs (platform services are in idp-platform).
S3_ENV = [
    k8s.V1EnvVar(name="IDP_S3_ENDPOINT", value=MINIO_ENDPOINT),
    k8s.V1EnvVar(name="IDP_S3_ACCESS_KEY", value="minioadmin"),
    k8s.V1EnvVar(name="IDP_S3_SECRET_KEY", value="minioadmin123"),
    k8s.V1EnvVar(name="IDP_S3_BUCKET_PREFIX", value=BUCKET_PREFIX),
]
# TimescaleDB env for the serving (publish) stage.
TS_ENV = [
    k8s.V1EnvVar(name="IDP_TS_HOST", value="timescaledb.idp-platform.svc"),
    k8s.V1EnvVar(name="IDP_TS_PORT", value="5432"),
    k8s.V1EnvVar(name="IDP_TS_DB", value="idp"),
    k8s.V1EnvVar(name="IDP_TS_USER", value="postgres"),
    k8s.V1EnvVar(name="IDP_TS_PASSWORD", value="idp-dev-password"),
]

default_args = {"owner": "idp", "retries": 1, "retry_delay": timedelta(minutes=3)}


def _stage(task_id: str, arg: str, *, extra_env=None) -> KubernetesPodOperator:
    """A medallion stage on the pandas `idp` image (single-node execution)."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"idp-{arg}",
        namespace=APP_NS,
        image=IMAGE,
        image_pull_policy="Never",          # local image built into minikube
        cmds=["idp"],
        arguments=[arg],
        env_vars=S3_ENV + (extra_env or []),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )


def _gold_spark() -> KubernetesPodOperator:
    """The Gold stage on Spark: spark-submit the PySpark job in an apache/spark pod.

    Mirrors deploy/app/jobs/spark-gold-job.yaml: reads Silver, writes Gold on
    MinIO via S3A; the script is mounted from the `idp-gold-spark` ConfigMap.
    Same task_id ("gold") as the pandas path, so the DAG shape is unchanged.
    """
    return KubernetesPodOperator(
        task_id="gold",
        name="idp-spark-gold",
        namespace=APP_NS,
        image=SPARK_IMAGE,
        image_pull_policy="IfNotPresent",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--conf", f"spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT}",
            "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
            "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin123",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
            "/opt/idp/gold_job.py", "--prefix", BUCKET_PREFIX,
        ],
        env_vars=[k8s.V1EnvVar(name="HOME", value="/tmp")],  # writable Ivy cache home
        volumes=[k8s.V1Volume(
            name="job-script",
            config_map=k8s.V1ConfigMapVolumeSource(name="idp-gold-spark"),
        )],
        volume_mounts=[k8s.V1VolumeMount(name="job-script", mount_path="/opt/idp")],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1500Mi"},
            limits={"cpu": "2", "memory": "2500Mi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )


def _gold_task() -> KubernetesPodOperator:
    """Gold stage on the configured engine (pandas | spark) — same task_id."""
    return _gold_spark() if _gold_engine() == "spark" else _stage("gold", "gold")


with DAG(
    dag_id="idp_silver_gold",
    description="Medallion refresh: Silver -> Gold -> publish to serving DB",
    schedule="0 * * * *",                   # hourly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["idp", "medallion"],
) as dag_refresh:
    _stage("silver", "silver") >> _gold_task() >> _stage("publish", "publish", extra_env=TS_ENV)


with DAG(
    dag_id="idp_train",
    description="Offline ML training on labelled data",
    schedule="0 2 * * *",                   # daily 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["idp", "ml"],
) as dag_train:
    _stage("train", "train")
