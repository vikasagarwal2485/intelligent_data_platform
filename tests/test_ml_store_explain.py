"""Phase 12 — model store, fee-charge features, and SHAP/scorecard reason codes.

Covers REQ-ML-04 (shared model store), the fee-charge model features (ADR-0023),
and REQ-ML-01/03.2 adverse-action reason codes.
"""
from __future__ import annotations

from idp.common import storage
from idp.common.config import PlatformConfig, StorageConfig, Tenant
from idp.features.credit_features import CREDIT_FEATURES, build_credit_features
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.pipeline import to_gold


def _local_cfg(tmp_path):
    sc = StorageConfig(
        root=str(tmp_path), landing=str(tmp_path / "l"), bronze=str(tmp_path / "b"),
        silver=str(tmp_path / "s"), gold=str(tmp_path / "g"), ml_artifacts=str(tmp_path / "ml"),
    )
    return PlatformConfig(raw={"s3": {"enabled": False}, "features": {"windows": [30, 90]}},
                          tenants=[Tenant("tenant_a", "A", "eu", "salt-a")], storage=sc)


# --- REQ-ML-04: model store -------------------------------------------------
def test_model_store_roundtrip_local(tmp_path):
    cfg = _local_cfg(tmp_path)
    uri = storage.model_uri(cfg, "m.joblib")
    assert not uri.startswith("s3://")
    assert storage.model_exists(uri, cfg) is False
    storage.save_model({"a": 1}, uri, cfg)
    assert storage.model_exists(uri, cfg) is True
    assert storage.load_model(uri, cfg) == {"a": 1}


def test_model_uri_is_s3_bucket_when_enabled():
    cfg = PlatformConfig(
        raw={"s3": {"enabled": True, "bucket_prefix": "idp", "endpoint": "http://x",
                    "access_key": "a", "secret_key": "b"}},
        tenants=[], storage=StorageConfig("", "", "", "", "", ""))
    assert storage.model_uri(cfg, "credit_model.joblib") == "s3://idp-ml/credit_model.joblib"


# --- fee-charge features fed to the credit model (ADR-0023) ------------------
def test_fee_features_present_and_attributed(silver):
    feats = build_credit_features(silver)
    for c in ("fee_total", "fee_recurring_total", "fee_overage_count",
              "fee_charge_count", "fee_total_30d"):
        assert c in feats.columns
    # fees are attributed to subscribers (agreement -> service), so some are > 0
    assert (feats["fee_total"] > 0).any()
    assert (feats["fee_charge_count"] > 0).any()


# --- REQ-ML-01/03.2: adverse-action reason codes ----------------------------
def test_reason_codes_are_adverse_and_labelled(credit_labeled):
    df = credit_labeled
    model = CreditLimitModel().fit(df, df["will_default"])
    codes = model.reason_codes(df.head(5), top_k=3)
    assert len(codes) == 5
    for row in codes:
        assert len(row) <= 3
        for r in row:
            assert r["feature"] in CREDIT_FEATURES
            assert r["contribution"] > 0            # adverse = raises PD
            assert r["method"] in ("shap", "scorecard")


# --- end-to-end: Gold loads models from the store and emits scores ----------
def test_gold_loads_models_from_store(tmp_path, silver, credit_labeled, fraud_labeled):
    cfg = _local_cfg(tmp_path)
    cm = CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"])
    fm = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    storage.save_model(cm, storage.model_uri(cfg, "credit_model.joblib"), cfg)
    storage.save_model(fm, storage.model_uri(cfg, "fraud_model.joblib"), cfg)

    # to_gold gets NO models in-memory -> must load them from the store
    result = to_gold(cfg, silver=silver, persist=False)
    assert "credit_scores" in result["gold_tables"]
    assert "fraud_scores" in result["gold_tables"]
