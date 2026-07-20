"""Airflow DAGs that orchestrate the medallion stages (production mode).

Airflow is the ORCHESTRATOR (schedule / order / retries); each stage runs as a
POD it launches. Silver AND Gold each have an EXECUTION-ENGINE switch, so either
stage can run single-node (pandas) or distributed (Spark) independently — the DAG
shape (silver >> gold >> publish) is identical regardless:

  * pandas — launches `idp silver` / `idp gold` in the scoring-api image.
  * spark  — launches `spark-submit` of pipelines/spark/{silver,gold}_job.py in an
             apache/spark pod (script mounted from the idp-{silver,gold}-spark CM).

Engines (independent):
  * idp_silver_engine  (Variable, else env IDP_SILVER_ENGINE)  default `pandas`
  * idp_gold_engine    (Variable, else env IDP_GOLD_ENGINE)    default `pandas`
Set them with `make silver-engine ENGINE=...` / `make gold-engine ENGINE=...`, or
in the Airflow UI (Admin -> Variables). Spark paths need the script ConfigMaps
(`make spark-configmap`).

Publish is a SEPARATE, independent task (Gold -> TimescaleDB) — never folded into
Gold. Next step: migrate its target from TimescaleDB to Iceberg tables via the
Nessie catalog (the serving read path changes, the task stays independent).

This is the correct decoupling: Airflow only needs permission to launch pods — it
does NOT need the `idp` package or Spark installed. The pods carry code + deps.

Prerequisites:
  * `idp/scoring-api:dev` on the node (`make image`) for the pandas paths
  * for spark: the idp-silver-spark / idp-gold-spark ConfigMaps (`make spark-configmap`)
  * the Airflow SA may create pods in `idp-app` (deploy/platform/airflow/rbac.yaml)

DAGs:
  * idp_silver_gold : silver >> gold >> score >> publish   (hourly refresh)
  * idp_train       : labels >> train                      (daily; fits on Gold
                      features + the labels store, no synthetic regen)
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
# Per-tenant PII salts for the Spark Silver job (DEV values; match config/platform.yaml
# and the pandas path so tokens are identical). Source from a Secret on EKS.
SPARK_SALTS = "tenant_a=salt-a-CHANGE-ME,tenant_b=salt-b-CHANGE-ME"


def _engine(variable: str, env: str) -> str:
    """Resolve a stage engine: Airflow Variable, else env var, else 'pandas'.
    Wrapped so a metadata-DB hiccup at parse time can't break DAG import."""
    try:
        from airflow.models import Variable
        val = Variable.get(variable, default_var=os.environ.get(env, "pandas"))
    except Exception:
        val = os.environ.get(env, "pandas")
    return (val or "pandas").strip().lower()


# S3/MinIO env every pandas stage pod needs (platform services are in idp-platform).
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
        on_finish_action="delete_succeeded_pod",  # keep FAILED stage pods in idp-app for debugging
        in_cluster=True,
    )


def _spark_stage(task_id: str, name: str, script: str, configmap: str,
                 *, extra_args=None) -> KubernetesPodOperator:
    """A medallion stage on Spark: spark-submit pipelines/spark/<script> in an
    apache/spark pod (mirrors deploy/app/jobs/spark-<stage>-job.yaml). Same
    task_id as the pandas variant, so the DAG shape is unchanged."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=name,
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
            f"/opt/idp/{script}", "--prefix", BUCKET_PREFIX, *(extra_args or []),
        ],
        env_vars=[k8s.V1EnvVar(name="HOME", value="/tmp")],  # writable Ivy cache home
        volumes=[k8s.V1Volume(
            name="job-script",
            config_map=k8s.V1ConfigMapVolumeSource(name=configmap),
        )],
        volume_mounts=[k8s.V1VolumeMount(name="job-script", mount_path="/opt/idp")],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1500Mi"},
            limits={"cpu": "2", "memory": "2500Mi"},
        ),
        get_logs=True,
        on_finish_action="delete_succeeded_pod",  # keep FAILED stage pods in idp-app for debugging
        in_cluster=True,
    )


def _silver_task() -> KubernetesPodOperator:
    """Bronze -> Silver on the configured engine (pandas | spark) — same task_id."""
    if _engine("idp_silver_engine", "IDP_SILVER_ENGINE") == "spark":
        return _spark_stage("silver", "idp-spark-silver", "silver_job.py",
                            "idp-silver-spark", extra_args=["--salts", SPARK_SALTS])
    return _stage("silver", "silver")


def _gold_task() -> KubernetesPodOperator:
    """Silver -> Gold on the configured engine (pandas | spark) — same task_id."""
    if _engine("idp_gold_engine", "IDP_GOLD_ENGINE") == "spark":
        return _spark_stage("gold", "idp-spark-gold", "gold_job.py", "idp-gold-spark")
    return _stage("gold", "gold")


# DAG 1 — refresh: Silver -> Gold (features) -> Score (inference) -> Publish.
# Gold builds features only; Score applies the trained models from the store; the
# two are separate tasks so feature-build and inference are decoupled.
with DAG(
    dag_id="idp_silver_gold",
    description="Medallion refresh: Silver -> Gold -> Score -> publish to serving DB",
    schedule="0 * * * *",                   # hourly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["idp", "medallion"],
) as dag_refresh:
    (_silver_task() >> _gold_task() >> _stage("score", "score")
     >> _stage("publish", "publish", extra_env=TS_ENV))


# DAG 2 — training: fit the models on historical Gold features + the labels store,
# on its own (daily) cadence. The `labels` task seeds the outcomes store — in this
# prototype it stands in for the business's real observed-outcomes feed; in
# production replace it with that feed and keep just `train`.
with DAG(
    dag_id="idp_train",
    description="Offline ML training on Gold features + labels store",
    schedule="0 2 * * *",                   # daily 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["idp", "ml"],
) as dag_train:
    _stage("labels", "labels") >> _stage("train", "train")
