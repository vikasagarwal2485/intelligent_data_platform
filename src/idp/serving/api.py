"""FastAPI scoring service (plan 5.6 / 6.6).

Exposes on-demand credit-limit and fraud scoring. Every request must carry a
``tenant_id`` (in production this is derived from the validated OIDC token via
Keycloak; here it is an explicit field) and the service only ever returns a
score for that tenant's subject - enforcing tenant isolation at the edge.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from idp.common.config import load_config
from idp.common.feature_contract import CREDIT_FEATURES, FRAUD_FEATURES
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.ml.registry import model_store


class ScoreRequest(BaseModel):
    tenant_id: str = Field(..., description="Caller's tenant; enforces isolation")
    service_id: str = Field(..., description="Tokenized subscriber id (MSISDN token)")
    features: dict[str, float] = Field(default_factory=dict)


def _to_frame(req: ScoreRequest, feature_list: list[str]) -> pd.DataFrame:
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
    def credit_limit(req: ScoreRequest) -> dict[str, Any]:
        frame = _to_frame(req, CREDIT_FEATURES)
        model = _credit()
        result = model.recommend_limit(frame).iloc[0].to_dict()
        result["reason_codes"] = model.reason_codes(frame, top_k=3)[0]  # adverse-action codes
        return result

    @app.post("/fraud-score")
    def fraud_score(req: ScoreRequest) -> dict[str, Any]:
        frame = _to_frame(req, FRAUD_FEATURES)
        result = _fraud().score(frame).iloc[0].to_dict()
        return result

    @app.get("/serving/{table}/{tenant_id}/{service_id}")
    def serving_lookup(table: str, tenant_id: str, service_id: str) -> dict[str, Any]:
        """Low-latency lookup of a PRECOMPUTED Gold row from TimescaleDB.

        e.g. GET /serving/credit_scores/tenant_a/tok_abc — returns the score the
        Gold+publish stages materialized, without recomputing from features.
        """
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
