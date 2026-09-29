"""Scoring-API authentication and tenant scoping (REQ-SRV-03).

A generated RSA keypair stands in for the Keycloak realm: tokens are minted
locally, and the JWKS fetch of the real ``PyJWKClient`` is stubbed, so its key
lookup and refetch-on-unknown-kid logic still run. No Keycloak needed.
"""
from __future__ import annotations

import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from fastapi.testclient import TestClient

from idp.common import storage
from idp.common.feature_contract import CREDIT_FEATURES, FRAUD_FEATURES
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.serving import auth

ISSUER = "http://keycloak.idp-platform.svc:8080/realms/idp"
AUDIENCE = "idp-scoring-api"


def _keypair(kid: str):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    return pem, {**jwk, "kid": kid, "alg": "RS256", "use": "sig"}


REALM_PEM, REALM_JWK = _keypair("realm-key")
OTHER_PEM, OTHER_JWK = _keypair("attacker-key")


def token(tenant: str | None = "tenant_a", *, pem=REALM_PEM, kid="realm-key", exp_in=300,
          iss=ISSUER, aud=AUDIENCE, **extra) -> str:
    now = int(time.time())
    claims = {"iss": iss, "aud": aud, "iat": now, "exp": now + exp_in, "sub": "u1", **extra}
    if tenant is not None:
        claims["tenant_id"] = tenant
    return jwt.encode(claims, pem, algorithm="RS256", headers={"kid": kid})


@pytest.fixture()
def jwks(monkeypatch):
    """The realm's JWKS as served by Keycloak; ``fetches`` counts the fetches."""
    state = {"keys": [REALM_JWK], "fetches": 0, "down": False}

    def fetch_data(self):
        state["fetches"] += 1
        if state["down"]:
            raise jwt.PyJWKClientConnectionError("connection refused")
        return {"keys": list(state["keys"])}
    monkeypatch.setattr(jwt.PyJWKClient, "fetch_data", fetch_data)
    return state


@pytest.fixture()
def verifier(jwks):
    return auth.TokenVerifier({"issuer": ISSUER, "audience": AUDIENCE, "tenant_claim": "tenant_id",
                               "leeway_seconds": 30})


def _status(fn) -> int:
    try:
        fn()
    except HTTPException as e:
        return e.status_code
    return 200


# --------------------------------------------------------------------------- #
# Token validation (task 2.1)                                                  #
# --------------------------------------------------------------------------- #
def test_valid_token_gives_its_tenant(verifier):
    assert verifier.tenant(token("tenant_b")) == "tenant_b"


@pytest.mark.parametrize("bad", [
    lambda: token(exp_in=-120),                              # expired (beyond the 30 s leeway)
    lambda: token(pem=OTHER_PEM, kid="attacker-key"),        # signed by a key the realm doesn't have
    lambda: token(pem=OTHER_PEM, kid="realm-key"),           # forged with the realm's kid
    lambda: token(iss="http://evil/realms/idp"),             # another issuer
    lambda: token(aud="some-other-client"),                  # not for this API
    lambda: "not-a-jwt",
])
def test_invalid_tokens_are_401(verifier, bad):
    assert _status(lambda: verifier.tenant(bad())) == 401


def test_expiry_leeway(verifier):
    assert verifier.tenant(token(exp_in=-10)) == "tenant_a"   # within 30 s


def test_token_without_tenant_claim_is_403(verifier):
    assert _status(lambda: verifier.tenant(token(None))) == 403


def test_unknown_kid_refetches_the_jwks(verifier, jwks):
    verifier.tenant(token())                                 # caches the realm key
    rotated_pem, rotated_jwk = _keypair("rotated-key")
    jwks["keys"].append(rotated_jwk)                         # Keycloak rotated its key
    before = jwks["fetches"]
    assert verifier.tenant(token(pem=rotated_pem, kid="rotated-key")) == "tenant_a"
    assert jwks["fetches"] > before


def test_unreachable_jwks_is_503_never_a_pass(verifier, jwks):
    jwks["down"] = True
    assert _status(lambda: verifier.tenant(token())) == 503


def test_auth_needs_an_issuer():
    with pytest.raises(ValueError):
        auth.TokenVerifier({"issuer": "", "audience": AUDIENCE})


def test_scoped_tenant():
    assert auth.scoped_tenant("tenant_a", None) == "tenant_a"          # auth off: the request's
    assert auth.scoped_tenant("", "tenant_a") == "tenant_a"            # omitted: the token's
    assert auth.scoped_tenant("tenant_a", "tenant_a") == "tenant_a"
    assert _status(lambda: auth.scoped_tenant("tenant_b", "tenant_a")) == 403
    assert _status(lambda: auth.scoped_tenant("", None)) == 422


# --------------------------------------------------------------------------- #
# The API with auth on (tasks 2.2 / 2.3)                                       #
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def artifacts(credit_labeled, fraud_labeled, tmp_path_factory):
    """A local model store with trained credit and fraud models."""
    d = tmp_path_factory.mktemp("ml")
    cfg = type("C", (), {"s3": {}})()
    storage.save_model(CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"]),
                       str(d / "credit_model.joblib"), cfg)
    storage.save_model(FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"]),
                       str(d / "fraud_model.joblib"), cfg)
    return d


@pytest.fixture()
def auth_env(monkeypatch, jwks):
    monkeypatch.setenv("IDP_AUTH_ENABLED", "true")
    monkeypatch.setenv("IDP_AUTH_ISSUER", ISSUER)
    monkeypatch.setenv("IDP_AUTH_AUDIENCE", AUDIENCE)
    monkeypatch.delenv("IDP_MLFLOW_TRACKING_URI", raising=False)


def _client(artifacts_dir):
    from idp.serving.api import create_app
    return TestClient(create_app(artifacts_dir=artifacts_dir))


def _bearer(t: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {t}"}


CREDIT = {"service_id": "tok_x", "features": {f: 1.0 for f in CREDIT_FEATURES}}
FRAUD = {"service_id": "tok_x", "features": {f: 1.0 for f in FRAUD_FEATURES}}


def test_health_stays_open(auth_env, artifacts):
    # SCN-SRV-03.5
    assert _client(artifacts).get("/health").json() == {"status": "ok"}


def test_missing_or_invalid_token_is_401_and_computes_nothing(auth_env, artifacts, monkeypatch):
    # SCN-SRV-03.3
    from idp.ml import registry
    monkeypatch.setattr(registry.ObjectModelStore, "load_champion",
                        lambda *a: pytest.fail("a rejected request must not load a model"))
    client = _client(artifacts)
    body = {"tenant_id": "tenant_a", **CREDIT}
    for headers in ({}, {"Authorization": "Basic dXNlcjpwdw=="}, _bearer(token(exp_in=-120)),
                    _bearer(token(pem=OTHER_PEM, kid="attacker-key"))):
        resp = client.post("/credit-limit", json=body, headers=headers)
        assert resp.status_code == 401, headers
        assert resp.headers.get("www-authenticate") == "Bearer"


def test_cross_tenant_requests_are_403(auth_env, artifacts, monkeypatch):
    # SCN-SRV-03.4 — path and body; no tenant_b data is read
    from idp.serving import timescale
    monkeypatch.setenv("IDP_TS_HOST", "localhost")
    monkeypatch.setattr(timescale, "read_score",
                        lambda *a, **k: pytest.fail("must not read another tenant's row"))
    client = _client(artifacts)
    a = _bearer(token("tenant_a"))
    assert client.get("/serving/credit_scores/tenant_b/tok_x", headers=a).status_code == 403
    assert client.post("/credit-limit", json={"tenant_id": "tenant_b", **CREDIT}, headers=a).status_code == 403
    assert client.post("/fraud-score", json={"tenant_id": "tenant_b", **FRAUD}, headers=a).status_code == 403


def test_own_tenant_lookup_is_served(auth_env, artifacts, monkeypatch):
    from idp.serving import timescale
    row = {"tenant_id": "tenant_a", "service_id": "tok_x", "recommended_limit": 42.0}
    seen = []
    monkeypatch.setenv("IDP_TS_HOST", "localhost")
    monkeypatch.setattr(timescale, "read_score", lambda cfg, table, t, s: seen.append(t) or row)
    resp = _client(artifacts).get("/serving/credit_scores/tenant_a/tok_x",
                                  headers=_bearer(token("tenant_a")))
    assert resp.status_code == 200 and resp.json() == row and seen == ["tenant_a"]


def test_valid_token_scores_for_its_tenant(auth_env, artifacts):
    # SCN-SRV-03.2 — with or without the body's tenant_id, the response is the token's tenant
    client = _client(artifacts)
    h = _bearer(token("tenant_a"))
    r1 = client.post("/credit-limit", json=CREDIT, headers=h)
    r2 = client.post("/credit-limit", json={"tenant_id": "tenant_a", **CREDIT}, headers=h)
    r3 = client.post("/fraud-score", json=FRAUD, headers=h)
    assert (r1.status_code, r2.status_code, r3.status_code) == (200, 200, 200)
    assert r1.json()["tenant_id"] == r2.json()["tenant_id"] == r3.json()["tenant_id"] == "tenant_a"
    assert "recommended_limit" in r1.json() and "fraud_score" in r3.json()


def test_valid_token_before_models_exist_is_503(auth_env, tmp_path):
    # SCN-SRV-03.1
    resp = _client(tmp_path / "empty").post("/credit-limit", json=CREDIT,
                                            headers=_bearer(token("tenant_a")))
    assert resp.status_code == 503


def test_token_without_tenant_claim_is_403_at_the_api(auth_env, artifacts):
    resp = _client(artifacts).post("/credit-limit", json=CREDIT, headers=_bearer(token(None)))
    assert resp.status_code == 403


def test_unreachable_keycloak_is_503(auth_env, artifacts, jwks):
    jwks["down"] = True
    resp = _client(artifacts).post("/credit-limit", json=CREDIT, headers=_bearer(token("tenant_a")))
    assert resp.status_code == 503


def test_dev_mode_needs_the_body_tenant(monkeypatch, artifacts):
    # auth off (local dev/tests): the request's tenant_id, as before
    monkeypatch.delenv("IDP_AUTH_ENABLED", raising=False)
    monkeypatch.delenv("IDP_MLFLOW_TRACKING_URI", raising=False)
    client = _client(artifacts)
    assert client.post("/credit-limit", json={"tenant_id": "tenant_b", **CREDIT}).json()["tenant_id"] == "tenant_b"
    assert client.post("/credit-limit", json=CREDIT).status_code == 422
