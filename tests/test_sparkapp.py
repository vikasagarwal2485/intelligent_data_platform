"""SparkApplication manifests for the Spark stages (REQ-PLT-10, REQ-ORC-07)."""
from __future__ import annotations

from pathlib import Path

import pytest

from idp.common.config import load_config
from idp.transforms.sparkapp import K8S_MASTER, SPARK_STAGES, render, spark_manifest
from idp.transforms.session import memory_mib, stage_memory_mib

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("stage", SPARK_STAGES)
def test_committed_manifests_match_the_profiles(stage):
    # `make spark-manifests` output must not drift from config/platform.yaml
    rendered = render(load_config(), stage)
    assert (ROOT / "deploy/app/spark" / f"{stage}.yaml").read_text() == rendered


def test_manifests_have_one_copy():
    # SCN-ORC-07.4 — deploy/app/spark/ is the only copy: the Airflow image bakes it in as dags/spark/
    # (named build context), so the DAG folder must not carry its own
    assert not (ROOT / "pipelines/airflow/dags/spark").exists()
    dockerfile = (ROOT / "deploy/platform/airflow/Dockerfile").read_text()
    assert "COPY --from=spark" in dockerfile and "/opt/airflow/dags/spark/" in dockerfile
    makefile = (ROOT / "Makefile").read_text()
    assert makefile.count("--build-context spark=deploy/app/spark") == 2   # airflow-wire + airflow-reload


@pytest.mark.parametrize("stage", SPARK_STAGES)
def test_resources_and_env_follow_the_profile(stage):
    cfg = load_config()
    spec = spark_manifest(cfg, stage)["spec"]
    prof = cfg.spark_profile(stage)
    drv, exe = spec["driver"], spec["executor"]
    assert (drv["memory"], drv["memoryOverhead"]) == (prof["driver"]["memory"], prof["driver"]["memoryOverhead"])
    assert exe["instances"] == prof["executor"]["instances"]
    assert (exe["memory"], exe["memoryOverhead"]) == (prof["executor"]["memory"], prof["executor"]["memoryOverhead"])
    # container limit = heap + overhead (REQ-ORC-07), the same arithmetic mk-check uses
    need = stage_memory_mib(cfg, stage)
    assert need["driver"] == memory_mib(drv["memory"]) + memory_mib(drv["memoryOverhead"])
    env = {e["name"]: e["value"] for e in drv["env"]}
    assert env["IDP_SPARK_MASTER"] == K8S_MASTER            # the driver keeps the operator's master
    assert env["IDP_CATALOG_TYPE"] == "sql"
    assert ("IDP_TS_HOST" in env) == (stage == "publish")     # serving env only where needed
    assert spec["mainApplicationFile"] == f"local:///app/pipelines/spark/{stage}_job.py"
    assert (ROOT / "pipelines/spark" / f"{stage}_job.py").exists()


def test_stage_args_and_unknown_stage():
    assert spark_manifest(load_config(), "silver", ["--full-refresh"])["spec"]["arguments"] == ["--full-refresh"]
    with pytest.raises(ValueError):
        spark_manifest(load_config(), "consume")


def test_one_cluster_path_per_spark_stage():
    # SCN-ORC-01.9 — each Spark stage runs in the cluster only as its rendered
    # SparkApplication; the only Kubernetes Jobs are the non-Spark stages `make` applies
    import re
    import yaml

    jobs = sorted((ROOT / "deploy/app/jobs").glob("*.yaml"))
    for job in jobs:
        for doc in yaml.safe_load_all(job.read_text()):
            if doc and doc.get("kind") == "Job":
                [container] = doc["spec"]["template"]["spec"]["containers"]
                assert container["command"][:2][-1] not in SPARK_STAGES, job.name
    applied = set(re.findall(r"deploy/app/jobs/([\w-]+\.yaml)", (ROOT / "Makefile").read_text()))
    assert {j.name for j in jobs} == applied == {
        "generate-job.yaml", "produce-job.yaml", "produce-from-landing-job.yaml",
        "consume-job.yaml", "labels-job.yaml"}
    for stage in SPARK_STAGES:
        assert (ROOT / "deploy/app/spark" / f"{stage}.yaml").exists(), stage
