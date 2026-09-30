"""The model store: MLflow registry with a gated champion, and the object-store
fallback (REQ-ML-04).

The MLflow tests use a local sqlite tracking/registry store with a tmp artifact
root, so they need no MLflow server.
"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from idp.common.config import PlatformConfig, StorageConfig, Tenant
from idp.common.feature_contract import CREDIT_FEATURES
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.ml.registry import (MlflowModelStore, ObjectModelStore, gate, model_store,
                             registered_name)

PASS = {"credit": {"n": 50, "auc": 0.8, "default_rate": 0.2},
        "fraud": {"n": 50, "pr_auc": 0.5, "fraud_rate": 0.1}}
FAIL = {"credit": {"n": 50, "auc": 0.52, "default_rate": 0.2},
        "fraud": {"n": 50, "pr_auc": 0.12, "fraud_rate": 0.1}}


def _cfg(tmp_path, tracking_uri: str = "") -> PlatformConfig:
    sc = StorageConfig(root=str(tmp_path), landing=str(tmp_path / "l"), bronze=str(tmp_path / "b"),
                       silver=str(tmp_path / "s"), gold=str(tmp_path / "g"),
                       ml_artifacts=str(tmp_path / "ml"))
    raw = {"s3": {"enabled": False}, "features": {"windows": [30, 90]},
           "credit": {"min_auc": 0.6}, "fraud": {"min_pr_auc_lift": 1.5},
           "mlflow": {"tracking_uri": tracking_uri, "experiment": "idp-test",
                      "artifact_location": (tmp_path / "mlartifacts").as_uri(),
                      "refresh_seconds": 60}}
    return PlatformConfig(raw=raw, tenants=[Tenant("tenant_a", "A", "eu", "salt-a")], storage=sc)


@pytest.fixture()
def no_env(monkeypatch):
    for var in ("IDP_MLFLOW_TRACKING_URI", "IDP_MLFLOW_REFRESH_SECONDS"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture()
def mlflow_cfg(tmp_path, no_env):
    return _cfg(tmp_path, f"sqlite:///{tmp_path / 'mlflow.db'}")


@pytest.fixture(scope="module")
def models(credit_labeled, fraud_labeled):
    cm = CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"])
    fm = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    return cm, fm


def _save(store, models, metrics, credit=None):
    cm, fm = models
    return store.save({"credit": (credit or cm, metrics["credit"]), "fraud": (fm, metrics["fraud"])},
                      {"train_max_rows": 1000, "sample_seed": 42})


# --------------------------------------------------------------------------- #
# Gate                                                                         #
# --------------------------------------------------------------------------- #
def test_gate_thresholds(tmp_path):
    cfg = _cfg(tmp_path)
    assert gate(cfg, "credit", PASS["credit"]).passed
    assert not gate(cfg, "credit", FAIL["credit"]).passed
    assert gate(cfg, "fraud", PASS["fraud"]).passed               # 0.5 >= 1.5 x 0.1
    assert not gate(cfg, "fraud", FAIL["fraud"]).passed           # 0.12 < 0.15
    # a single-class hold-out has no AUC / PR-AUC: the gate fails
    assert not gate(cfg, "credit", {"n": 10, "default_rate": 0.0}).passed
    assert not gate(cfg, "fraud", {"n": 10, "fraud_rate": 0.0}).passed


# --------------------------------------------------------------------------- #
# Object store (fallback) — SCN-ML-04.6                                        #
# --------------------------------------------------------------------------- #
def test_object_store_is_the_fallback(tmp_path, no_env, models):
    cfg = _cfg(tmp_path)
    store = model_store(cfg)
    assert isinstance(store, ObjectModelStore)
    assert store.load_champion("credit") is None and store.champion_version("credit") is None
    out = _save(store, models, FAIL)              # no gate in the fallback: latest wins
    assert out["credit"].promoted and not out["credit"].gate.passed
    assert isinstance(store.load_champion("credit"), CreditLimitModel)
    assert isinstance(store.load_champion("fraud"), FraudModel)
    v1 = store.champion_version("credit")
    time.sleep(0.01)
    _save(store, models, PASS)
    assert store.champion_version("credit") not in (None, v1)   # a new save is a new version


# --------------------------------------------------------------------------- #
# MLflow registry — SCN-ML-04.2 / 04.3 / 04.4                                  #
# --------------------------------------------------------------------------- #
def test_training_run_is_logged_and_registered(mlflow_cfg, models):
    # SCN-ML-04.2
    store = model_store(mlflow_cfg)
    assert isinstance(store, MlflowModelStore)
    out = _save(store, models, PASS)
    assert (out["credit"].version, out["fraud"].version) == ("1", "1")
    c = store.client
    mv = c.get_model_version(registered_name("credit"), "1")
    run = c.get_run(mv.run_id)
    assert run.data.metrics["credit_auc"] == 0.8 and run.data.metrics["fraud_pr_auc"] == 0.5
    assert run.data.params["train_max_rows"] == "1000"
    assert c.get_model_version(registered_name("fraud"), "1").run_id == mv.run_id   # same run
    # task 3.3 — which code trained it
    assert run.data.tags["idp.version"] and run.data.tags["idp.git_sha"]
    assert run.data.tags["credit.gate"] == "passed"


def test_gate_passing_model_becomes_champion(mlflow_cfg, models):
    # SCN-ML-04.3
    store = model_store(mlflow_cfg)
    _save(store, models, PASS)
    assert store.champion_version("credit") == "1" and store.champion_version("fraud") == "1"
    model = store.load_champion("credit")
    assert isinstance(model, CreditLimitModel)                   # unwrapped, not a pyfunc
    import pandas as pd
    frame = pd.DataFrame([{"tenant_id": "t", "service_id": "s", **{f: 1.0 for f in CREDIT_FEATURES}}])
    assert "recommended_limit" in model.recommend_limit(frame).columns
    assert model.reason_codes(frame, top_k=2)


def test_gate_failing_model_is_registered_not_promoted(mlflow_cfg, models):
    # SCN-ML-04.4
    store = model_store(mlflow_cfg)
    _save(store, models, PASS)
    out = _save(store, models, FAIL)
    assert out["credit"].version == "2" and not out["credit"].promoted
    mv = store.client.get_model_version(registered_name("credit"), "2")
    assert mv.tags["gate"] == "failed" and float(mv.tags["metric.credit_auc"]) == 0.52
    assert store.champion_version("credit") == "1" and store.champion_version("fraud") == "1"


def test_no_champion_yet_skips_the_model(mlflow_cfg, models):
    # SCN-ML-04.4 — the first version failed its gate: nothing to score with
    from idp.transforms.scoring import load_scoring_models
    store = model_store(mlflow_cfg)
    _save(store, models, FAIL)
    assert store.champion_version("credit") is None and store.load_champion("credit") is None
    assert load_scoring_models(mlflow_cfg) == {}


def test_score_uses_the_champion(tmp_path, no_env, lake, models):
    # SCN-ML-04.1 / 04.3 — Score (no in-memory models) loads the champions
    from conftest import clone_lake
    from idp.pipeline import to_scores
    cfg = clone_lake(lake, tmp_path / "lake", layers=("gold",),
                     tables={"credit_features", "fraud_features"})
    cfg.raw["mlflow"] = _cfg(tmp_path, f"sqlite:///{tmp_path / 'mlflow.db'}").raw["mlflow"]
    store = model_store(cfg)
    _save(store, models, {"credit": PASS["credit"], "fraud": FAIL["fraud"]})
    result = to_scores(cfg, persist=False)
    assert "credit_scores" in result["score_tables"]
    assert "fraud_scores" not in result["score_tables"]         # no fraud champion yet


# --------------------------------------------------------------------------- #
# API refresh — SCN-ML-04.5                                                    #
# --------------------------------------------------------------------------- #
def test_api_picks_up_a_new_champion_without_restart(mlflow_cfg, models, monkeypatch):
    # SCN-ML-04.5 — on a fake clock, so the refresh interval is exact: registering a
    # version takes real seconds, which a wall-clock interval can't be shorter than
    from idp.serving import api

    clock = [1000.0]
    monkeypatch.setattr(api.time, "monotonic", lambda: clock[0])
    store = model_store(mlflow_cfg)
    cm, _ = models
    _save(store, models, PASS)
    monkeypatch.setenv("IDP_MLFLOW_TRACKING_URI", mlflow_cfg.mlflow["tracking_uri"])
    monkeypatch.setenv("IDP_MLFLOW_REFRESH_SECONDS", "60")
    client = TestClient(api.create_app())
    payload = {"tenant_id": "tenant_a", "service_id": "tok_x",
               "features": {f: 1.0 for f in CREDIT_FEATURES}}
    first = client.post("/credit-limit", json=payload).json()["recommended_limit"]

    # promote a version that recommends different limits (a higher floor)
    import copy
    bigger = copy.deepcopy(cm)
    bigger.min_limit = first + 7.0
    assert _save(store, models, PASS, credit=bigger)["credit"].promoted
    clock[0] += 59                                                  # within the interval
    assert client.post("/credit-limit", json=payload).json()["recommended_limit"] == first  # cached
    clock[0] += 2                                                   # past it
    assert client.post("/credit-limit", json=payload).json()["recommended_limit"] != first  # new champion

    # an unreachable registry keeps serving the cached model (stale, not 503)
    def down(self, name):
        raise ConnectionError("registry down")
    monkeypatch.setattr(MlflowModelStore, "champion_version", down)
    clock[0] += 61
    assert client.post("/credit-limit", json=payload).status_code == 200


def test_api_without_champion_is_503(mlflow_cfg, models, monkeypatch):
    store = model_store(mlflow_cfg)
    _save(store, models, FAIL)
    monkeypatch.setenv("IDP_MLFLOW_TRACKING_URI", mlflow_cfg.mlflow["tracking_uri"])
    from idp.serving import api
    client = TestClient(api.create_app())
    resp = client.post("/credit-limit", json={"tenant_id": "a", "service_id": "x", "features": {}})
    assert resp.status_code == 503
