"""Tests for feature engineering (plan 4) — the Gold feature tables built by the
Spark Gold stage (conftest `lake`)."""
from __future__ import annotations

from idp.features import CREDIT_FEATURES, FRAUD_FEATURES


def test_credit_features_shape_and_columns(silver, credit_features):
    # SCN-GLD-01.1
    assert set(["tenant_id", "service_id"] + CREDIT_FEATURES).issubset(credit_features.columns)
    assert len(credit_features) == len(silver["dim_subscriber"])
    assert credit_features[CREDIT_FEATURES].notna().all().all()


def test_fraud_features_shape_and_columns(fraud_features):
    assert set(["tenant_id", "service_id"] + FRAUD_FEATURES).issubset(fraud_features.columns)
    assert fraud_features[FRAUD_FEATURES].notna().all().all()


def test_originating_ratio_between_zero_and_one(fraud_features):
    # SCN-GLD-01.2
    assert fraud_features["originating_ratio"].between(0, 1).all()
    assert fraud_features["roaming_share"].between(0, 1).all()


def test_features_are_tenant_isolated(credit_features):
    # one tenant's features never reference another tenant's subscribers
    assert set(credit_features["tenant_id"]) == {"tenant_a", "tenant_b"}


def test_recharge_success_rate_bounded(credit_features):
    assert credit_features["recharge_success_rate"].between(0, 1).all()
