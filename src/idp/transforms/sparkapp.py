"""SparkApplication manifests for the Spark stages (REQ-ORC-01/07, REQ-PLT-10).

In the cluster, Silver, Gold, Score, publish and maintenance run as Spark
Operator ``SparkApplication``s: one driver pod plus executor pods, all from the
``idp`` image, running the same stage code as the CLI. The resources come from
``spark.profiles.<stage>`` in ``config/platform.yaml``, so profiles live in one
place. ``idp spark-manifest <stage>`` renders the manifest; the ``make`` targets
and the DAG image use the rendered files (``deploy/app/spark/<stage>.yaml``).
"""
from __future__ import annotations

from typing import Any

import yaml

from idp.common.config import PlatformConfig

SPARK_STAGES = ("silver", "gold", "score", "publish", "maintain")
NAMESPACE = "idp-app"
IMAGE = "idp/scoring-api:dev"
SPARK_VERSION = "3.5.3"
K8S_MASTER = "k8s://https://kubernetes.default.svc:443"
SERVICE_ACCOUNT = "idp-spark"

# The platform endpoints the stage pods use (the same values as the Kubernetes
# Jobs and the DAG; dev credentials, External Secrets on EKS).
LAKE_ENV = {
    "IDP_S3_ENDPOINT": "http://minio.idp-platform.svc:9000",
    "IDP_S3_ACCESS_KEY": "minioadmin",
    "IDP_S3_SECRET_KEY": "minioadmin123",
    "IDP_S3_BUCKET_PREFIX": "idp",
    "IDP_CATALOG_TYPE": "sql",
    "IDP_CATALOG_URI": "postgresql+psycopg2://postgres:idp-dev-password@timescaledb.idp-platform.svc:5432/iceberg",
    # the driver runs get_spark(): keep the operator's Kubernetes master (not local[*])
    "IDP_SPARK_MASTER": K8S_MASTER,
}
SERVING_ENV = {
    "IDP_TS_HOST": "timescaledb.idp-platform.svc",
    "IDP_TS_PORT": "5432",
    "IDP_TS_DB": "idp",
    "IDP_TS_USER": "postgres",
    "IDP_TS_PASSWORD": "idp-dev-password",
}


def app_name(stage: str) -> str:
    return f"idp-{stage}"


def _env(stage: str) -> list[dict[str, str]]:
    env = dict(LAKE_ENV)
    if stage == "publish":
        env.update(SERVING_ENV)
    return [{"name": k, "value": v} for k, v in env.items()]


def spark_manifest(cfg: PlatformConfig, stage: str, args: list[str] | None = None) -> dict[str, Any]:
    """The ``SparkApplication`` for ``stage`` (one of ``SPARK_STAGES``).

    Each container's limit is heap + overhead (REQ-ORC-07): the operator sizes the
    driver and executor pods from ``memory`` + ``memoryOverhead``.
    """
    if stage not in SPARK_STAGES:
        raise ValueError(f"not a Spark stage: {stage!r}; expected one of {SPARK_STAGES}")
    prof = cfg.spark_profile(stage)
    drv, exe = prof["driver"], prof["executor"]
    labels = {"app": "idp-medallion", "stage": stage}
    env = _env(stage)
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": app_name(stage), "namespace": NAMESPACE, "labels": dict(labels)},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": IMAGE,
            "imagePullPolicy": "Never",                   # built into minikube (`make image`)
            "mainApplicationFile": f"local:///app/pipelines/spark/{stage}_job.py",
            "arguments": list(args or []),
            "sparkVersion": SPARK_VERSION,
            "restartPolicy": {"type": "Never"},          # Airflow / make own retries
            "timeToLiveSeconds": 3600,
            "driver": {
                "cores": int(drv.get("cores", 1)),
                "memory": str(drv.get("memory", cfg.spark["driver_memory"])),
                "memoryOverhead": str(drv.get("memoryOverhead", "512m")),
                "serviceAccount": SERVICE_ACCOUNT,
                "labels": dict(labels),
                "env": [dict(e) for e in env],
            },
            "executor": {
                "instances": int(exe.get("instances", 1)),
                "cores": int(exe.get("cores", 1)),
                "memory": str(exe.get("memory", "1g")),
                "memoryOverhead": str(exe.get("memoryOverhead", "512m")),
                "labels": dict(labels),
                "env": [dict(e) for e in env],
            },
        },
    }


def render(cfg: PlatformConfig, stage: str, args: list[str] | None = None) -> str:
    header = (f"# Rendered by `idp spark-manifest {stage}` from spark.profiles.{stage} in\n"
              "# config/platform.yaml. Don't edit by hand: `make spark-manifests` re-renders.\n")
    return header + yaml.safe_dump(spark_manifest(cfg, stage, args), sort_keys=False)
