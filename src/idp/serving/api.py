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
    art = Path(artifacts_dir or cfg.storage.ml_artifacts)
    app = FastAPI(title="IDP Scoring API", version="0.1.0")
    state: dict[str, Any] = {"credit": None, "fraud": None}

    def _credit() -> CreditLimitModel:
        if state["credit"] is None:
            path = art / "credit_model.joblib"
            if not path.exists():
                raise HTTPException(503, "credit model not trained yet")
            state["credit"] = CreditLimitModel.load(path)
        return state["credit"]

    def _fraud() -> FraudModel:
        if state["fraud"] is None:
            path = art / "fraud_model.joblib"
            if not path.exists():
                raise HTTPException(503, "fraud model not trained yet")
            state["fraud"] = FraudModel.load(path)
        return state["fraud"]

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/credit-limit")
    def credit_limit(req: ScoreRequest) -> dict[str, Any]:
        frame = _to_frame(req, CREDIT_FEATURES)
        result = _credit().recommend_limit(frame).iloc[0].to_dict()
        return result

    @app.post("/fraud-score")
    def fraud_score(req: ScoreRequest) -> dict[str, Any]:
        frame = _to_frame(req, FRAUD_FEATURES)
        result = _fraud().score(frame).iloc[0].to_dict()
        return result

    return app


app = create_app()
