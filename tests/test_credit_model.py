"""Tests for the credit-limit model."""
from __future__ import annotations

import numpy as np

from idp.ml.credit_model import CreditLimitModel, heuristic_limit, ks_statistic


def test_heuristic_limit_within_policy_bands(credit_labeled):
    limits = heuristic_limit(credit_labeled, min_limit=5, max_limit=500)
    assert (limits >= 5).all()
    assert (limits <= 500).all()


def test_heuristic_increases_with_recharge():
    import pandas as pd
    df = pd.DataFrame({
        "recharge_avg_amount": [10, 100],
        "customer_tenure_days": [400, 400],
        "recharge_success_rate": [1.0, 1.0],
        "is_company": [0, 0],
    })
    limits = heuristic_limit(df)
    assert limits.iloc[1] > limits.iloc[0]


def test_pd_model_trains_and_scores(credit_labeled):
    model = CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"])
    pd_hat = model.predict_pd(credit_labeled)
    assert pd_hat.shape[0] == len(credit_labeled)
    assert ((pd_hat >= 0) & (pd_hat <= 1)).all()


def test_pd_model_has_signal(credit_labeled):
    """In-sample AUC should be clearly better than random."""
    model = CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"])
    metrics = model.evaluate(credit_labeled, credit_labeled["will_default"])
    assert metrics["auc"] > 0.65


def test_recommend_limit_outputs(credit_labeled):
    model = CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"])
    out = model.recommend_limit(credit_labeled)
    assert {"pd", "recommended_limit", "affordability_base"}.issubset(out.columns)
    assert out["recommended_limit"].between(5, 500).all()
    # higher PD should not increase the recommended limit on average
    hi = out[out["pd"] > out["pd"].median()]["recommended_limit"].mean()
    lo = out[out["pd"] <= out["pd"].median()]["recommended_limit"].mean()
    assert hi <= lo


def test_ks_statistic_range():
    y = np.array([0, 0, 1, 1])
    s = np.array([0.1, 0.2, 0.8, 0.9])
    assert ks_statistic(y, s) == 1.0


def test_save_and_load(credit_labeled, tmp_path):
    model = CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"])
    path = tmp_path / "credit.joblib"
    model.save(path)
    loaded = CreditLimitModel.load(path)
    np.testing.assert_allclose(
        model.predict_pd(credit_labeled), loaded.predict_pd(credit_labeled)
    )
