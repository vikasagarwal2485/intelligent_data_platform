"""Tests for the fraud model (plan 6)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from idp.ml.fraud_model import FraudModel, rule_flags


def test_rule_flags_simbox():
    df = pd.DataFrame({
        "tenant_id": ["a"], "service_id": ["x"],
        "originating_ratio": [0.95], "calls_per_active_day": [250],
        "failed_recharge_count": [0], "roaming_share": [0.0],
    })
    flags = rule_flags(df)
    assert flags["rule_simbox"].iloc[0] == 1
    assert flags["rule_score"].iloc[0] == 1.0


def test_rule_flags_recharge_fraud():
    df = pd.DataFrame({
        "tenant_id": ["a"], "service_id": ["x"],
        "originating_ratio": [0.1], "calls_per_active_day": [5],
        "failed_recharge_count": [9], "roaming_share": [0.0],
    })
    flags = rule_flags(df)
    assert flags["rule_recharge_fraud"].iloc[0] == 1


def test_fraud_model_trains_and_scores(fraud_labeled):
    model = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    scored = model.score(fraud_labeled)
    assert {"fraud_proba", "anomaly_score", "rule_score", "fraud_score"}.issubset(scored.columns)
    assert scored["fraud_score"].between(0, 1).all()


def test_fraud_model_has_signal(fraud_labeled):
    model = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    metrics = model.evaluate(fraud_labeled, fraud_labeled["is_fraud"])
    assert metrics["pr_auc"] > fraud_labeled["is_fraud"].mean()  # beats base rate


def test_fraud_score_ranks_fraud_higher(fraud_labeled):
    model = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    scored = model.score(fraud_labeled)
    merged = scored.merge(
        fraud_labeled[["tenant_id", "service_id", "is_fraud"]],
        on=["tenant_id", "service_id"],
    )
    fraud_mean = merged[merged.is_fraud]["fraud_score"].mean()
    normal_mean = merged[~merged.is_fraud]["fraud_score"].mean()
    assert fraud_mean > normal_mean


def test_save_and_load(fraud_labeled, tmp_path):
    model = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    path = tmp_path / "fraud.joblib"
    model.save(path)
    loaded = FraudModel.load(path)
    np.testing.assert_allclose(
        model.predict_proba(fraud_labeled), loaded.predict_proba(fraud_labeled)
    )
