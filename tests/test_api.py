"""Tests for the FastAPI scoring service (plan 5.6 / 6.6)."""
from __future__ import annotations

from fastapi.testclient import TestClient

from idp.common.config import StorageConfig, load_config
from idp.features import CREDIT_FEATURES, FRAUD_FEATURES
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
