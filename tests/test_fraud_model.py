"""Tests for the fraud model."""
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


def test_default_score_unchanged_and_global_range_reproduces_whole_frame(fraud_labeled):
    # task 2.1 / SCN-ML-07.5 groundwork
    model = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    whole = model.score(fraud_labeled)
    raw = model.raw_anomaly(fraud_labeled)
    # default = normalize over the frame; passing the frame's own range is the same
    pd.testing.assert_frame_equal(whole, model.score(fraud_labeled, anomaly_range=(raw.min(), raw.max())))
    # scoring in partitions with the POPULATION range equals scoring the whole frame
    rng = (raw.min(), raw.max())
    parts = [model.score(fraud_labeled.iloc[i::3], anomaly_range=rng) for i in range(3)]
    stitched = pd.concat(parts).sort_values(["tenant_id", "service_id"]).reset_index(drop=True)
    expected = whole.sort_values(["tenant_id", "service_id"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(stitched, expected)
    # ...whereas per-partition normalization (the old behaviour on a partition) differs
    naive = pd.concat(model.score(fraud_labeled.iloc[i::3]) for i in range(3))
    naive = naive.sort_values(["tenant_id", "service_id"]).reset_index(drop=True)
    assert not np.allclose(naive["anomaly_score"], expected["anomaly_score"])


def test_training_population_range_is_stored_and_saved(fraud_labeled, tmp_path):
    # the range a single on-demand request is scored on: the raw anomaly min/max over
    # the training rows, kept with the model
    model = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    raw = model.raw_anomaly(fraud_labeled)
    assert model.anomaly_range == (float(raw.min()), float(raw.max()))
    path = tmp_path / "fraud.joblib"
    model.save(path)
    assert FraudModel.load(path).anomaly_range == model.anomaly_range


def test_single_row_scored_on_the_training_range(fraud_labeled):
    # one subscriber alone has no population: without a range its anomaly score is 0;
    # with the stored training range it matches its place in the training population
    model = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    whole = model.score(fraud_labeled, anomaly_range=model.anomaly_range)
    i = int(whole["anomaly_score"].idxmax())
    alone = model.score(fraud_labeled.iloc[[i]], anomaly_range=model.anomaly_range).iloc[0]
    assert model.score(fraud_labeled.iloc[[i]]).iloc[0]["anomaly_score"] == 0.0
    assert alone["anomaly_score"] == whole.loc[i, "anomaly_score"] > 0.5
    assert alone["fraud_score"] == whole.loc[i, "fraud_score"]
    # outside the training range: clipped to the 0-1 scale
    extreme = fraud_labeled.iloc[[i]].copy()
    for col in model.features:
        extreme[col] = extreme[col].astype(float) * 50
    assert 0.0 <= model.score(extreme, anomaly_range=model.anomaly_range).iloc[0]["anomaly_score"] <= 1.0
