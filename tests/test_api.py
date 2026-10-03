"""Tests for the FastAPI scoring service (plan 5.6 / 6.6)."""
from __future__ import annotations

import pytest

from fastapi.testclient import TestClient

from idp.common.config import StorageConfig, load_config
from idp.common.feature_contract import CREDIT_FEATURES, FRAUD_FEATURES
from idp.pipeline import run
from idp.serving.api import create_app


def _train_models(tmp_path):
    cfg = load_config()
    cfg.raw["synthetic"].update({"subscribers_per_tenant": 100, "days": 25, "fraud_rate": 0.15})
    cfg.storage = StorageConfig(
        root=str(tmp_path), landing=str(tmp_path / "l"), bronze=str(tmp_path / "b"),
        silver=str(tmp_path / "s"), gold=str(tmp_path / "g"), ml_artifacts=str(tmp_path / "ml"),
    )
    run(cfg, persist=True)
    return tmp_path / "ml"


def test_health():
    client = TestClient(create_app())
    assert client.get("/health").json() == {"status": "ok"}


def test_credit_limit_endpoint(tmp_path):
    art = _train_models(tmp_path)
    client = TestClient(create_app(artifacts_dir=art))
    payload = {
        "tenant_id": "tenant_a", "service_id": "tok_test",
        "features": {f: 1.0 for f in CREDIT_FEATURES},
    }
    resp = client.post("/credit-limit", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert "recommended_limit" in body and "pd" in body
    assert body["tenant_id"] == "tenant_a"


def test_fraud_score_endpoint(tmp_path):
    art = _train_models(tmp_path)
    client = TestClient(create_app(artifacts_dir=art))
    payload = {
        "tenant_id": "tenant_b", "service_id": "tok_test",
        "features": {f: 1.0 for f in FRAUD_FEATURES},
    }
    resp = client.post("/fraud-score", json=payload)
    assert resp.status_code == 200
    assert "fraud_score" in resp.json()


def test_fraud_score_uses_the_training_range(tmp_path):
    # a single on-demand request gets a real anomaly score, scaled on the training
    # population's range stored with the model (it used to be always 0)
    art = _train_models(tmp_path)
    client = TestClient(create_app(artifacts_dir=art))
    features = {f: 1000.0 for f in FRAUD_FEATURES}                 # far from typical behaviour
    body = client.post("/fraud-score", json={"tenant_id": "tenant_b", "service_id": "tok_x",
                                             "features": features}).json()
    assert 0.0 < body["anomaly_score"] <= 1.0
    assert body["fraud_score"] == pytest.approx(min(1.0, 0.6 * body["fraud_proba"] + 0.2 * body["anomaly_score"]
                                                     + 0.2 * body["rule_score"]), abs=1e-3)


def test_unknown_feature_names_are_rejected(tmp_path):
    # SCN-SRV-02.4: the former feature name is a 422 naming it, before any model is
    # loaded (no artifacts here)
    client = TestClient(create_app(artifacts_dir=tmp_path / "none"))
    payload = {"tenant_id": "tenant_a", "service_id": "tok_test",
               "features": {"recharge_avg_amount": 40, "account_tenure_days": 500}}
    resp = client.post("/credit-limit", json=payload)
    assert resp.status_code == 422
    assert resp.json()["detail"]["unknown_features"] == ["account_tenure_days"]
    resp = client.post("/fraud-score", json={**payload, "features": {"recharge_max": 1, "bogus": 2}})
    assert resp.status_code == 422 and resp.json()["detail"]["unknown_features"] == ["bogus"]


def test_missing_features_default_to_zero(tmp_path):
    # SCN-SRV-02.5: a partial feature map is still scored
    art = _train_models(tmp_path)
    client = TestClient(create_app(artifacts_dir=art))
    resp = client.post("/fraud-score", json={"tenant_id": "tenant_b", "service_id": "tok_test",
                                             "features": {"recharge_max": 10.0}})
    assert resp.status_code == 200 and "fraud_score" in resp.json()


def test_missing_model_returns_503(tmp_path):
    client = TestClient(create_app(artifacts_dir=tmp_path / "empty"))
    resp = client.post("/credit-limit", json={"tenant_id": "a", "service_id": "x", "features": {}})
    assert resp.status_code == 503


# --- REQ-SRV-04: precomputed serving lookup ---------------------------------
def test_serving_lookup_503_when_store_disabled(monkeypatch):
    monkeypatch.delenv("IDP_TS_HOST", raising=False)
    client = TestClient(create_app())
    assert client.get("/serving/credit_scores/tenant_a/tok_x").status_code == 503


def test_serving_lookup_404_for_unknown_table_or_row(monkeypatch):
    from idp.serving import timescale

    monkeypatch.setenv("IDP_TS_HOST", "localhost")
    monkeypatch.setattr(timescale, "read_score", lambda *a, **k: None)
    client = TestClient(create_app())
    assert client.get("/serving/labels/tenant_a/tok_x").status_code == 404
    assert client.get("/serving/credit_scores/tenant_a/tok_missing").status_code == 404


def test_serving_lookup_returns_published_row(monkeypatch):
    from idp.serving import timescale

    row = {"tenant_id": "tenant_a", "service_id": "tok_x", "recommended_limit": 42.0}
    monkeypatch.setenv("IDP_TS_HOST", "localhost")
    monkeypatch.setattr(timescale, "read_score", lambda *a, **k: row)
    resp = TestClient(create_app()).get("/serving/credit_scores/tenant_a/tok_x")
    assert resp.status_code == 200 and resp.json() == row


def test_deployed_api_reads_the_serving_store_publish_writes():
    # REQ-SRV-04 — the scoring-api Deployment enables the lookup (IDP_TS_HOST) and
    # points at the same TimescaleDB/database/user as the publish stage
    from pathlib import Path

    import yaml

    from idp.transforms.sparkapp import SERVING_ENV

    root = Path(__file__).resolve().parents[1]
    [dep] = [d for d in yaml.safe_load_all((root / "deploy/app/deployment/scoring-api.yaml").read_text())
             if d and d["kind"] == "Deployment"]
    [container] = dep["spec"]["template"]["spec"]["containers"]
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert {k: env.get(k) for k in SERVING_ENV} == SERVING_ENV


def test_timescale_off_without_host(monkeypatch):
    # the serving store is enabled by IDP_TS_HOST, never by default
    from idp.common.config import PlatformConfig, StorageConfig
    monkeypatch.delenv("IDP_TS_HOST", raising=False)
    assert load_config().timescale["enabled"] is False
    bare = PlatformConfig(raw={}, tenants=[], storage=StorageConfig("", "", "", "", "", ""))
    assert bare.timescale["enabled"] is False
    monkeypatch.setenv("IDP_TS_HOST", "db")
    assert load_config().timescale["enabled"] is True
