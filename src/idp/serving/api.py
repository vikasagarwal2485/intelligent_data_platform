"""FastAPI scoring service.

Exposes on-demand credit-limit and fraud scoring and the precomputed serving
lookup. With ``auth.enabled`` (every cluster deployment), each endpoint except
``/health`` needs a Keycloak Bearer token and serves only the token's tenant: a
``tenant_id`` in the body or path that differs is a 403 (``serving/auth.py``,
REQ-SRV-03). With auth off (local dev, tests), the request's ``tenant_id`` is used.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from idp.common.config import load_config
from idp.common.feature_contract import CREDIT_FEATURES, FRAUD_FEATURES
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.ml.registry import model_store
from idp.serving.auth import scoped_tenant, tenant_dependency


class ScoreRequest(BaseModel):
    tenant_id: str = Field("", description="Caller's tenant. With auth on it may be "
                           "omitted (the token's tenant is used) and must match it if given")
    service_id: str = Field(..., description="Tokenized subscriber id (MSISDN token)")
    features: dict[str, float] = Field(default_factory=dict)


def _to_frame(req: ScoreRequest, feature_list: list[str]) -> pd.DataFrame:
    """The model's input row. Missing features are 0; a feature name outside the
    model's contract is a 422 naming it (REQ-SRV-02), so a misspelled or renamed
    feature (e.g. the former ``account_tenure_days``) can't be silently scored as 0."""
    unknown = sorted(set(req.features) - set(feature_list))
    if unknown:
        raise HTTPException(422, {"message": "unknown feature names (not in the model's feature contract)",
                                  "unknown_features": unknown})
    row: dict[str, Any] = {"tenant_id": req.tenant_id, "service_id": req.service_id}
    for f in feature_list:
        row[f] = float(req.features.get(f, 0.0))
    return pd.DataFrame([row])


def create_app(artifacts_dir: str | Path | None = None) -> FastAPI:
    """Application factory. Loads models lazily so the app starts without them."""
    cfg = load_config()
    app = FastAPI(title="IDP Scoring API", version="0.1.0")
    # The model store's champion (REQ-ML-04): MLflow `champion`, or the object
    # store's latest file; an explicit artifacts_dir (tests) is a local object store.
    store = model_store(cfg, artifacts_dir=artifacts_dir)
    # The caller's tenant from the Bearer token (None when auth is off).
    current_tenant = tenant_dependency(cfg.auth)
    refresh = float(cfg.mlflow.get("refresh_seconds", 60))
    # per model: (version, model, time of the last champion check)
    cache: dict[str, tuple[str | None, Any, float]] = {}

    def _champion(name: str) -> Any:
        """The cached champion, re-checked every ``refresh`` seconds. A new version
        is loaded without a restart; a failed check or load keeps the cached model
        (serve stale rather than 503)."""
        now = time.monotonic()
        version, model, checked = cache.get(name, (None, None, float("-inf")))
        if model is not None and now - checked < refresh:
            return model
        try:
            current = store.champion_version(name)
            if model is None or current != version:
                loaded = store.load_champion(name) if current is not None else None
                if loaded is not None:
                    version, model = current, loaded
        except Exception:  # noqa: BLE001 - registry/object store unreachable
            if model is None:
                raise HTTPException(503, f"{name} model store unreachable")
        cache[name] = (version, model, now)
        if model is None:
            raise HTTPException(503, f"{name} model not trained yet (no champion)")
        return model

    def _credit() -> CreditLimitModel:
        return _champion("credit")

    def _fraud() -> FraudModel:
        return _champion("fraud")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/credit-limit")
    def credit_limit(req: ScoreRequest, token_tenant: str | None = Depends(current_tenant)) -> dict[str, Any]:
        req.tenant_id = scoped_tenant(req.tenant_id, token_tenant)
        frame = _to_frame(req, CREDIT_FEATURES)
        model = _credit()
        result = model.recommend_limit(frame).iloc[0].to_dict()
        result["reason_codes"] = model.reason_codes(frame, top_k=3)[0]  # adverse-action codes
        return result

    @app.post("/fraud-score")
    def fraud_score(req: ScoreRequest, token_tenant: str | None = Depends(current_tenant)) -> dict[str, Any]:
        req.tenant_id = scoped_tenant(req.tenant_id, token_tenant)
        frame = _to_frame(req, FRAUD_FEATURES)
        model = _fraud()
        # one subscriber has no population to scale the anomaly score against: use
        # the training population's range stored with the model (a model trained
        # before it was stored falls back to the request alone, i.e. 0)
        result = model.score(frame, anomaly_range=getattr(model, "anomaly_range", None)).iloc[0].to_dict()
        return result

    @app.get("/serving/{table}/{tenant_id}/{service_id}")
    def serving_lookup(table: str, tenant_id: str, service_id: str,
                       token_tenant: str | None = Depends(current_tenant)) -> dict[str, Any]:
        """Low-latency lookup of a PRECOMPUTED Gold row from TimescaleDB.

        e.g. GET /serving/credit_scores/tenant_a/tok_abc — returns the score the
        Gold+publish stages materialized, without recomputing from features. With
        auth on, the path's tenant must be the token's.
        """
        tenant_id = scoped_tenant(tenant_id, token_tenant)
        if not cfg.timescale.get("enabled"):
            raise HTTPException(503, "serving DB (TimescaleDB) not enabled")
        if table not in ("credit_scores", "fraud_scores", "credit_features", "fraud_features"):
            raise HTTPException(404, f"unknown serving table {table!r}")
        from idp.serving import timescale
        row = timescale.read_score(cfg, table, tenant_id, service_id)
        if row is None:
            raise HTTPException(404, "no serving row for that tenant/service_id")
        return row

    return app


app = create_app()
