"""Alternative credit-limit model (plan section 5).

Two-stage design:

* **Stage 0 - cold start** (:func:`heuristic_limit`): an affordability scorecard
  used before default labels exist.
* **Stage 1 - PD model** (:class:`CreditLimitModel`): a gradient-boosted
  probability-of-default classifier (sklearn ``HistGradientBoostingClassifier``,
  a drop-in for LightGBM/XGBoost from the plan) with a calibrated probability.
* **Stage 2 - limit sizing** (:meth:`CreditLimitModel.recommend_limit`):
  ``affordability x risk_multiplier(PD)`` clipped to policy bands.

A logistic-regression scorecard is trained alongside as the interpretable
regulatory baseline (plan 5.3).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from idp.common.feature_contract import CREDIT_FEATURES


def heuristic_limit(
    features: pd.DataFrame,
    *,
    base_multiplier: float = 3.0,
    min_limit: float = 5.0,
    max_limit: float = 500.0,
) -> pd.Series:
    """Stage-0 affordability heuristic (plan 5.2).

    ``limit0 = k * avg_recharge * tenure_factor * success_rate * segment_factor``.
    """
    avg_recharge = features.get("recharge_avg_amount", pd.Series(0, index=features.index)).fillna(0)
    tenure_factor = (features.get("account_tenure_days", 0).clip(lower=0) / 365.0).clip(upper=1.0)
    success = features.get("recharge_success_rate", pd.Series(0, index=features.index)).clip(0, 1)
    segment_factor = np.where(features.get("is_company", 0).astype(bool), 1.3, 1.0)
    limit = base_multiplier * avg_recharge * tenure_factor * success.clip(lower=0.1) * segment_factor
    return limit.clip(lower=min_limit, upper=max_limit).round(2)


def ks_statistic(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Kolmogorov-Smirnov separation between good/bad score distributions."""
    order = np.argsort(y_score)
    y = np.asarray(y_true)[order]
    pos = np.cumsum(y) / max(1, y.sum())
    neg = np.cumsum(1 - y) / max(1, (1 - y).sum())
    return float(np.max(np.abs(pos - neg)))


@dataclass
class CreditLimitModel:
    """Probability-of-default model + limit sizing."""

    base_multiplier: float = 3.0
    min_limit: float = 5.0
    max_limit: float = 500.0
    features: list[str] = field(default_factory=lambda: list(CREDIT_FEATURES))
    pd_model: HistGradientBoostingClassifier | None = None
    scorecard: Pipeline | None = None

    # --- training --------------------------------------------------------------
    def fit(self, X: pd.DataFrame, y: pd.Series | np.ndarray) -> "CreditLimitModel":
        Xm = X[self.features].astype(float)
        y = np.asarray(y).astype(int)
        self.pd_model = HistGradientBoostingClassifier(
            max_depth=3, learning_rate=0.05, max_iter=250, l2_regularization=1.0,
            min_samples_leaf=20, early_stopping=False, random_state=42,
        ).fit(Xm, y)
        # interpretable baseline scorecard
        self.scorecard = Pipeline([
            ("scale", StandardScaler()),
            ("lr", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]).fit(Xm, y)
        return self

    # --- inference -------------------------------------------------------------
    def predict_pd(self, X: pd.DataFrame) -> np.ndarray:
        """Probability of default."""
        if self.pd_model is None:
            raise RuntimeError("Model is not fitted")
        return self.pd_model.predict_proba(X[self.features].astype(float))[:, 1]

    def recommend_limit(self, X: pd.DataFrame) -> pd.DataFrame:
        """Stage-2: combine affordability and PD into a recommended limit."""
        pd_hat = self.predict_pd(X)
        affordability = heuristic_limit(
            X, base_multiplier=self.base_multiplier,
            min_limit=self.min_limit, max_limit=self.max_limit,
        )
        risk_multiplier = (1.0 - pd_hat)
        limit = (affordability.values * risk_multiplier)
        limit = np.clip(limit, self.min_limit, self.max_limit).round(2)
        out = X[["tenant_id", "service_id"]].copy()
        out["pd"] = pd_hat.round(4)
        out["affordability_base"] = affordability.values
        out["recommended_limit"] = limit
        return out.reset_index(drop=True)

    # --- explainability (adverse-action reason codes) --------------------------
    def _pd_contributions(self, Xm: pd.DataFrame) -> tuple[np.ndarray, str]:
        """Signed per-feature contributions to the probability/odds of default.

        Uses SHAP (TreeExplainer on the PD model) when the ``shap`` package is
        installed, else the interpretable logistic **scorecard** — the standard
        adverse-action method — via ``coef · standardized_feature``. Positive =
        pushes default risk UP (an adverse reason).
        """
        try:
            import shap  # optional; heavy, not a core dependency

            explainer = shap.TreeExplainer(self.pd_model)
            values = explainer.shap_values(Xm)
            arr = values[1] if isinstance(values, list) else values
            return np.asarray(arr, dtype=float), "shap"
        except Exception:  # noqa: BLE001 - fall back to the scorecard
            scaler = self.scorecard.named_steps["scale"]
            lr = self.scorecard.named_steps["lr"]
            z = (Xm.values - scaler.mean_) / scaler.scale_
            return z * lr.coef_[0], "scorecard"

    def reason_codes(self, X: pd.DataFrame, top_k: int = 3) -> list[list[dict]]:
        """Top-``k`` adverse-action reason codes per row (features raising PD most)."""
        Xm = X[self.features].astype(float)
        contribs, method = self._pd_contributions(Xm)
        reasons: list[list[dict]] = []
        for i in range(len(Xm)):
            row = contribs[i]
            order = np.argsort(row)[::-1][:top_k]
            reasons.append([
                {"feature": self.features[j], "contribution": round(float(row[j]), 4),
                 "method": method}
                for j in order if row[j] > 0
            ])
        return reasons

    # --- evaluation ------------------------------------------------------------
    def evaluate(self, X: pd.DataFrame, y: pd.Series | np.ndarray) -> dict[str, float]:
        y = np.asarray(y).astype(int)
        pd_hat = self.predict_pd(X)
        metrics = {"n": int(len(y)), "default_rate": float(y.mean())}
        if len(np.unique(y)) > 1:
            metrics["auc"] = float(roc_auc_score(y, pd_hat))
            metrics["ks"] = ks_statistic(y, pd_hat)
            metrics["gini"] = 2 * metrics["auc"] - 1
        return metrics

    # --- persistence -----------------------------------------------------------
    def save(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self, path)

    @staticmethod
    def load(path: str | Path) -> "CreditLimitModel":
        return joblib.load(path)
