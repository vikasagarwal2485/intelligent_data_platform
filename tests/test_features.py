"""Tests for feature engineering (plan 4)."""
from __future__ import annotations

from idp.features.credit_features import CREDIT_FEATURES, build_credit_features
from idp.features.fraud_features import FRAUD_FEATURES, build_fraud_features


def test_credit_features_shape_and_columns(silver):
    feats = build_credit_features(silver)
    assert set(["tenant_id", "service_id"] + CREDIT_FEATURES).issubset(feats.columns)
    assert len(feats) == len(silver["dim_subscriber"])
    assert feats[CREDIT_FEATURES].notna().all().all()


def test_fraud_features_shape_and_columns(silver):
    feats = build_fraud_features(silver)
    assert set(["tenant_id", "service_id"] + FRAUD_FEATURES).issubset(feats.columns)
    assert feats[FRAUD_FEATURES].notna().all().all()


def test_originating_ratio_between_zero_and_one(silver):
    feats = build_fraud_features(silver)
    assert feats["originating_ratio"].between(0, 1).all()
    assert feats["roaming_share"].between(0, 1).all()


def test_features_are_tenant_isolated(silver):
    feats = build_credit_features(silver)
    # one tenant's features never reference another tenant's subscribers
    assert set(feats["tenant_id"]) == {"tenant_a", "tenant_b"}


def test_recharge_success_rate_bounded(silver):
    feats = build_credit_features(silver)
    assert feats["recharge_success_rate"].between(0, 1).all()
