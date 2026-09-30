"""The Airflow DAG module, imported against minimal stubs (no Airflow install).

Checks the wiring the orchestration specs rely on: four DAGs, every task a
SparkApplication from its committed manifest with the lake env (S3 + Iceberg
catalog), a training DAG that is `train` only, and a maintenance DAG that triggers
nothing (SCN-ORC-02.5).
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).resolve().parents[1] / "pipelines" / "airflow" / "dags" / "idp_medallion_dag.py"
SPARK_MANIFESTS = Path(__file__).resolve().parents[1] / "deploy" / "app" / "spark"


class _Dataset:
    def __init__(self, uri):
        self.uri = uri

    def __or__(self, other):
        return ("or", self, other)


class _DAG:
    created: list["_DAG"] = []
    current: "_DAG | None" = None

    def __init__(self, dag_id, schedule=None, **kwargs):
        self.dag_id, self.schedule, self.kwargs, self.tasks = dag_id, schedule, kwargs, []
        _DAG.created.append(self)

    def __enter__(self):
        _DAG.current = self
        return self

    def __exit__(self, *exc):
        _DAG.current = None


class _Pod:
    def __init__(self, task_id, **kwargs):
        self.task_id, self.kwargs, self.outlets, self.downstream = task_id, kwargs, [], []
        _DAG.current.tasks.append(self)

    def __rshift__(self, other):
        self.downstream.append(other)
        return other


@pytest.fixture()
def dags(monkeypatch):
    _DAG.created = []
    mods = {
        "airflow": types.SimpleNamespace(DAG=_DAG),
        "airflow.datasets": types.SimpleNamespace(Dataset=_Dataset),
        "airflow.providers": types.SimpleNamespace(),
        "airflow.providers.cncf": types.SimpleNamespace(),
        "airflow.providers.cncf.kubernetes": types.SimpleNamespace(),
        "airflow.providers.cncf.kubernetes.operators": types.SimpleNamespace(),
        "airflow.providers.cncf.kubernetes.operators.spark_kubernetes":
            types.SimpleNamespace(SparkKubernetesOperator=_Pod),
    }
    for name, mod in mods.items():
        monkeypatch.setitem(sys.modules, name, mod)
    spec = importlib.util.spec_from_file_location("idp_medallion_dag", DAG_FILE)
    spec.loader.exec_module(importlib.util.module_from_spec(spec))
    return {d.dag_id: d for d in _DAG.created}


def test_four_dags(dags):
    assert set(dags) == {"idp_features", "idp_inference", "idp_training", "idp_maintenance"}


SPARK_TASKS = {"silver", "gold", "score", "publish", "maintain", "train"}


def test_airflow_only_submits_spark_applications(dags):
    # SCN-ORC-02.8 — every task is a SparkApplication from a committed manifest;
    # the DAG module doesn't even import the pod operator
    for dag in dags.values():
        for task in dag.tasks:
            assert set(task.kwargs) >= {"application_file", "namespace"}, task.task_id
            assert (SPARK_MANIFESTS / Path(task.kwargs["application_file"]).relative_to("spark")).exists()
    assert "KubernetesPodOperator" not in DAG_FILE.read_text()


def test_training_dag_is_train_only(dags):
    # REQ-ORC-02 — no labels task: labels are seeded outside Airflow (`make labels`)
    [task] = dags["idp_training"].tasks
    assert task.task_id == "train" and task.kwargs["application_file"] == "spark/train.yaml"
    assert [d.uri for d in task.outlets] == ["s3://idp-ml/credit_model.joblib"]   # triggers inference


def test_spark_stages_are_spark_applications(dags):
    # REQ-ORC-02 / SCN-ORC-02.6: each Spark stage submits its rendered SparkApplication
    import yaml
    seen = set()
    for dag in dags.values():
        for task in dag.tasks:
            if task.task_id not in SPARK_TASKS:
                continue
            seen.add(task.task_id)
            assert task.kwargs["namespace"] == "idp-app" and task.kwargs["delete_on_termination"]
            # the image's dags/spark/<stage>.yaml is baked from deploy/app/spark/ (the one copy)
            manifest = SPARK_MANIFESTS / Path(task.kwargs["application_file"]).relative_to("spark")
            spec = yaml.safe_load(manifest.read_text())["spec"]
            env = {e["name"]: e["value"] for e in spec["driver"]["env"]}
            assert env["IDP_CATALOG_TYPE"] == "sql" and env["IDP_S3_ENDPOINT"].startswith("http://minio.")
            assert env["IDP_CATALOG_URI"].endswith("@timescaledb.idp-platform.svc:5432/iceberg")
            assert env["IDP_MLFLOW_TRACKING_URI"].startswith("http://mlflow.")
            assert spec["executor"]["instances"] >= 1
    assert seen == SPARK_TASKS
    assert dags["idp_features"].kwargs["max_active_runs"] == 1


def test_maintenance_runs_daily_and_triggers_nothing(dags):
    # SCN-ORC-02.5
    m = dags["idp_maintenance"]
    assert m.schedule == "0 3 * * *" and m.kwargs["max_active_runs"] == 1
    [task] = m.tasks
    assert task.kwargs["application_file"] == "spark/maintain.yaml"
    assert not task.outlets and not task.downstream


def test_airflow_rbac_submits_applications_but_starts_no_pods():
    # REQ-PLT-10 / SCN-PLT-10.3 — the manifest's grants in idp-app
    import yaml
    docs = list(yaml.safe_load_all((DAG_FILE.parents[3] / "deploy/platform/airflow/rbac.yaml").read_text()))
    roles = [d for d in docs if d and d["kind"] == "Role"]
    grants = {(res, verb) for role in roles for rule in role["rules"]
              for res in rule["resources"] for verb in rule["verbs"]}
    assert {("sparkapplications", "create"), ("sparkapplications", "delete"),
            ("pods", "list"), ("pods/log", "get")} <= grants
    assert not {(r, v) for r, v in grants if r in ("pods", "pods/exec", "pods/log")
                and v in ("create", "delete", "patch", "update", "deletecollection")}
    assert not any(r == "pods/exec" for r, _ in grants)
    assert "idp-pod-launcher" not in {d["metadata"]["name"] for d in docs if d}
