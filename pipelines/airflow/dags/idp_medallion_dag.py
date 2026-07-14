"""Airflow DAGs that orchestrate the medallion stages (production mode).

Each task launches a POD running the `idp` image (`idp silver` / `idp gold` /
`idp train`) via KubernetesPodOperator. This is the correct decoupling: Airflow
only needs permission to launch pods — it does NOT need the `idp` package
installed. The pods carry the code + deps and write to MinIO/S3.

Prerequisites:
  * the `idp/scoring-api:dev` image is on the node (`make image`)
  * the Airflow service account may create pods in `idp-app`
    (apply deploy/platform/airflow/rbac.yaml; see docs/triggering.md)

DAGs:
  * idp_silver_gold : silver >> gold   (hourly)
  * idp_train       : train            (daily)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = "idp/scoring-api:dev"
APP_NS = "idp-app"

# S3/MinIO env every stage pod needs (platform services are in idp-platform).
S3_ENV = [
    k8s.V1EnvVar(name="IDP_S3_ENDPOINT", value="http://minio.idp-platform.svc:9000"),
    k8s.V1EnvVar(name="IDP_S3_ACCESS_KEY", value="minioadmin"),
    k8s.V1EnvVar(name="IDP_S3_SECRET_KEY", value="minioadmin123"),
    k8s.V1EnvVar(name="IDP_S3_BUCKET_PREFIX", value="idp"),
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


with DAG(
    dag_id="idp_silver_gold",
    description="Medallion refresh: Silver -> Gold -> publish to serving DB",
    schedule="0 * * * *",                   # hourly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["idp", "medallion"],
) as dag_refresh:
    _stage("silver", "silver") >> _stage("gold", "gold") >> _stage("publish", "publish", extra_env=TS_ENV)


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
