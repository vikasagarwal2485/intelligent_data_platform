"""Tests for the FastAPI scoring service."""
from __future__ import annotations

import pytest

from fastapi.testclient import TestClient

from idp.common.feature_contract import CREDIT_FEATURES, FRAUD_FEATURES
from idp.serving.api import create_app




def test_health():
    client = TestClient(create_app())
    assert client.get("/health").json() == {"status": "ok"}


def test_credit_limit_endpoint(model_artifacts):
    art = model_artifacts
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


def test_fraud_score_endpoint(model_artifacts):
    art = model_artifacts
    client = TestClient(create_app(artifacts_dir=art))
    payload = {
        "tenant_id": "tenant_b", "service_id": "tok_test",
        "features": {f: 1.0 for f in FRAUD_FEATURES},
    }
    resp = client.post("/fraud-score", json=payload)
    assert resp.status_code == 200
    assert "fraud_score" in resp.json()


def test_fraud_score_uses_the_training_range(model_artifacts):
    # a single on-demand request gets a real anomaly score, scaled on the training
    # population's range stored with the model (it used to be always 0)
    art = model_artifacts
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


def test_missing_features_default_to_zero(model_artifacts):
    # SCN-SRV-02.5: a partial feature map is still scored
    art = model_artifacts
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
    assert client.get("/serving/credit_scores/tok_x", headers={"X-Tenant-Id": "tenant_a"}).status_code == 503


def test_serving_lookup_404_for_unknown_table_or_row(monkeypatch):
    from idp.serving import timescale

    monkeypatch.setenv("IDP_TS_HOST", "localhost")
    monkeypatch.setattr(timescale, "read_score", lambda *a, **k: None)
    client = TestClient(create_app())
    h = {"X-Tenant-Id": "tenant_a"}
    assert client.get("/serving/labels/tok_x", headers=h).status_code == 404
    assert client.get("/serving/credit_scores/tok_missing", headers=h).status_code == 404


def test_serving_lookup_returns_published_row(monkeypatch):
    from idp.serving import timescale

    row = {"tenant_id": "tenant_a", "service_id": "tok_x", "recommended_limit": 42.0}
    monkeypatch.setenv("IDP_TS_HOST", "localhost")
    monkeypatch.setattr(timescale, "read_score", lambda *a, **k: row)
    seen = []
    monkeypatch.setattr(timescale, "read_score", lambda cfg, table, t, s: seen.append((table, t, s)) or row)
    resp = TestClient(create_app()).get("/serving/credit_scores/tok_x", headers={"X-Tenant-Id": "tenant_a"})
    assert resp.status_code == 200 and resp.json() == row
    assert seen == [("credit_scores", "tenant_a", "tok_x")]


def test_serving_lookup_needs_the_tenant_header_without_auth(monkeypatch):
    # auth off (dev, tests): the X-Tenant-Id header is the only source of the tenant
    monkeypatch.setenv("IDP_TS_HOST", "localhost")
    resp = TestClient(create_app()).get("/serving/credit_scores/tok_x")
    assert resp.status_code == 422 and "X-Tenant-Id" in resp.json()["detail"]


def test_old_lookup_path_is_gone():
    # the tenant is no longer a path segment
    assert TestClient(create_app()).get("/serving/credit_scores/tenant_a/tok_x").status_code == 404


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
    from idp.common.config import PlatformConfig, StorageConfig, load_config
    monkeypatch.delenv("IDP_TS_HOST", raising=False)
    assert load_config().timescale["enabled"] is False
    bare = PlatformConfig(raw={}, tenants=[], storage=StorageConfig("", "", "", "", "", ""))
    assert bare.timescale["enabled"] is False
    monkeypatch.setenv("IDP_TS_HOST", "db")
    assert load_config().timescale["enabled"] is True
