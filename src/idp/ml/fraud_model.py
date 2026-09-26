"""Fraud detection model (plan section 6).

Layered hybrid (batch-first per reviewer decision):

* **Layer 1 - rules** (:func:`rule_flags`): deterministic SIM-box / recharge
  signatures, zero training.
* **Layer 2 - supervised** (gradient-boosted classifier with balanced sample
  weights, optimized for PR-AUC).
* **Layer 3 - anomaly** (IsolationForest) for novel/unknown fraud.
* **Fusion**: combine the three into a single fraud score.

The Flink real-time variant of Layer 1 is deferred to a later phase; the rule
definitions here are intentionally portable.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, IsolationForest
from sklearn.metrics import average_precision_score
from sklearn.utils.class_weight import compute_sample_weight

from idp.features import FRAUD_FEATURES

DEFAULT_RULES = {
    "originating_ratio": 0.9,
    "max_calls_per_day": 200,
    "failed_recharge_burst": 5,
}


def rule_flags(features: pd.DataFrame, thresholds: dict | None = None) -> pd.DataFrame:
    """Layer 1: deterministic rule flags + a normalized rule score [0,1]."""
    t = {**DEFAULT_RULES, **(thresholds or {})}
    out = features[["tenant_id", "service_id"]].copy()
    out["rule_simbox"] = (
        (features.get("originating_ratio", 0) >= t["originating_ratio"])
        & (features.get("calls_per_active_day", 0) >= t["max_calls_per_day"])
    ).astype(int)
    out["rule_recharge_fraud"] = (
        features.get("failed_recharge_count", 0) >= t["failed_recharge_burst"]
    ).astype(int)
    out["rule_roaming_fraud"] = (features.get("roaming_share", 0) >= 0.5).astype(int)
    rule_cols = ["rule_simbox", "rule_recharge_fraud", "rule_roaming_fraud"]
    out["rule_score"] = out[rule_cols].max(axis=1).astype(float)
    return out


@dataclass
class FraudModel:
    """Supervised + anomaly + rules fraud detector."""

    features: list[str] = field(default_factory=lambda: list(FRAUD_FEATURES))
    contamination: float = 0.05
    thresholds: dict = field(default_factory=lambda: dict(DEFAULT_RULES))
    clf: HistGradientBoostingClassifier | None = None
    iforest: IsolationForest | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series | np.ndarray) -> "FraudModel":
        Xm = X[self.features].astype(float)
        y = np.asarray(y).astype(int)
        sample_weight = compute_sample_weight("balanced", y)
        self.clf = HistGradientBoostingClassifier(
            max_depth=4, learning_rate=0.1, max_iter=200, random_state=42,
        ).fit(Xm, y, sample_weight=sample_weight)
        self.iforest = IsolationForest(
            contamination=self.contamination, random_state=42, n_estimators=200,
        ).fit(Xm)
        return self

    def _anomaly_score(self, Xm: pd.DataFrame) -> np.ndarray:
        # higher = more anomalous, scaled to [0,1]
        raw = -self.iforest.score_samples(Xm)
        lo, hi = raw.min(), raw.max()
        return (raw - lo) / (hi - lo) if hi > lo else np.zeros_like(raw)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if self.clf is None:
            raise RuntimeError("Model is not fitted")
        return self.clf.predict_proba(X[self.features].astype(float))[:, 1]

    def score(self, X: pd.DataFrame) -> pd.DataFrame:
        """Fuse supervised proba, anomaly score and rules into a fraud score."""
        Xm = X[self.features].astype(float)
        proba = self.predict_proba(X)
        anomaly = self._anomaly_score(Xm)
        rules = rule_flags(X, self.thresholds)
        fused = 0.6 * proba + 0.2 * anomaly + 0.2 * rules["rule_score"].values
        out = X[["tenant_id", "service_id"]].copy()
        out["fraud_proba"] = proba.round(4)
        out["anomaly_score"] = anomaly.round(4)
        out["rule_score"] = rules["rule_score"].values
        out["fraud_score"] = np.clip(fused, 0, 1).round(4)
        return out.reset_index(drop=True)

    def evaluate(self, X: pd.DataFrame, y: pd.Series | np.ndarray, k: int = 20) -> dict[str, float]:
        y = np.asarray(y).astype(int)
        proba = self.predict_proba(X)
        metrics = {"n": int(len(y)), "fraud_rate": float(y.mean())}
        if len(np.unique(y)) > 1:
            metrics["pr_auc"] = float(average_precision_score(y, proba))
            k = min(k, len(y))
            top = np.argsort(proba)[::-1][:k]
            metrics[f"precision_at_{k}"] = float(y[top].mean())
        return metrics

    def save(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self, path)

    @staticmethod
    def load(path: str | Path) -> "FraudModel":
        return joblib.load(path)
