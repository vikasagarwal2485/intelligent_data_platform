"""FastAPI scoring service (plan 5.6 / 6.6).

Exposes on-demand credit-limit and fraud scoring. Every request must carry a
``tenant_id`` (in production this is derived from the validated OIDC token via
Keycloak; here it is an explicit field) and the service only ever returns a
score for that tenant's subject - enforcing tenant isolation at the edge.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from idp.common import storage
from idp.common.config import load_config
from idp.features.credit_features import CREDIT_FEATURES
from idp.features.fraud_features import FRAUD_FEATURES
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel


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
    state: dict[str, Any] = {"credit": None, "fraud": None}

    def _muri(name: str) -> str:
        # explicit override (tests) stays local; otherwise the model store
        # (MinIO s3://idp-ml in-cluster, local dir otherwise).
        if artifacts_dir is not None:
            return str(Path(artifacts_dir) / name)
        return storage.model_uri(cfg, name)

    def _credit() -> CreditLimitModel:
        if state["credit"] is None:
            uri = _muri("credit_model.joblib")
            if not storage.model_exists(uri, cfg):
                raise HTTPException(503, "credit model not trained yet")
            state["credit"] = storage.load_model(uri, cfg)
        return state["credit"]

    def _fraud() -> FraudModel:
        if state["fraud"] is None:
            uri = _muri("fraud_model.joblib")
            if not storage.model_exists(uri, cfg):
                raise HTTPException(503, "fraud model not trained yet")
            state["fraud"] = storage.load_model(uri, cfg)
        return state["fraud"]

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
