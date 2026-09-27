"""The Airflow DAG module, imported against minimal stubs (no Airflow install).

Checks the wiring the orchestration specs rely on: four DAGs, the lake env
(S3 + Iceberg catalog) on every stage pod, and a maintenance DAG that triggers
nothing (SCN-ORC-02.5).
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).resolve().parents[1] / "pipelines" / "airflow" / "dags" / "idp_medallion_dag.py"


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


class _EnvVar:
    def __init__(self, name, value):
        self.name, self.value = name, value


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
        "airflow.providers.cncf.kubernetes.operators.pod": types.SimpleNamespace(KubernetesPodOperator=_Pod),
        "kubernetes": types.SimpleNamespace(),
        "kubernetes.client": types.SimpleNamespace(
            models=types.SimpleNamespace(V1EnvVar=_EnvVar, V1ResourceRequirements=dict)),
    }
    for name, mod in mods.items():
        monkeypatch.setitem(sys.modules, name, mod)
    spec = importlib.util.spec_from_file_location("idp_medallion_dag", DAG_FILE)
    spec.loader.exec_module(importlib.util.module_from_spec(spec))
    return {d.dag_id: d for d in _DAG.created}


def test_four_dags(dags):
    assert set(dags) == {"idp_features", "idp_inference", "idp_training", "idp_maintenance"}


def test_every_stage_pod_has_the_lake_env(dags):
    for dag in dags.values():
        for task in dag.tasks:
            env = {e.name: e.value for e in task.kwargs["env_vars"]}
            assert env["IDP_CATALOG_TYPE"] == "sql", task.task_id
            assert env["IDP_CATALOG_URI"].startswith("postgresql+psycopg2://")
            assert env["IDP_CATALOG_URI"].endswith("@timescaledb.idp-platform.svc:5432/iceberg")
            assert env["IDP_S3_ENDPOINT"].startswith("http://minio.")


def test_maintenance_runs_daily_and_triggers_nothing(dags):
    # SCN-ORC-02.5
    m = dags["idp_maintenance"]
    assert m.schedule == "0 3 * * *" and m.kwargs["max_active_runs"] == 1
    [task] = m.tasks
    assert task.kwargs["arguments"] == ["maintain"] and not task.outlets and not task.downstream
    assert "IDP_SPARK_MASTER" in {e.name for e in task.kwargs["env_vars"]}
