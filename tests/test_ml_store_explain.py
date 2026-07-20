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
from idp.pipeline import to_gold, to_scores


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


# --- end-to-end: Score stage loads models from the store and emits scores ----
def test_score_loads_models_from_store(tmp_path, silver, credit_labeled, fraud_labeled):
    cfg = _local_cfg(tmp_path)
    cm = CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"])
    fm = FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])
    storage.save_model(cm, storage.model_uri(cfg, "credit_model.joblib"), cfg)
    storage.save_model(fm, storage.model_uri(cfg, "fraud_model.joblib"), cfg)

    gold = to_gold(cfg, silver=silver, persist=False)   # features only, no scores
    assert "credit_scores" not in gold["gold_tables"]
    # to_scores gets NO models in-memory -> must load them from the store
    result = to_scores(cfg, gold=gold, persist=False)
    assert "credit_scores" in result["score_tables"]
    assert "fraud_scores" in result["score_tables"]


# --- labels store + lake-path training (phase 13) ---------------------------
def test_labels_store_roundtrip(tmp_path):
    from idp.pipeline import _read_labels, to_labels
    from idp.synthetic.generator import generate_for_tenants
    cfg = _local_cfg(tmp_path)
    ds = generate_for_tenants(["tenant_a"], subscribers=40, days=30, seed=7)
    to_labels(cfg, dataset=ds)
    lbl = _read_labels(cfg)
    assert not lbl.empty
    assert {"tenant_id", "service_id", "will_default", "is_fraud"} <= set(lbl.columns)
    assert lbl["service_id"].astype(str).str.startswith("tok_").all()  # tokenized, no raw MSISDN


def test_train_reads_gold_and_labels_from_lake(tmp_path):
    from idp.pipeline import ENTITIES, to_bronze, to_gold, to_labels, to_silver, train
    from idp.synthetic.generator import generate_for_tenants
    cfg = _local_cfg(tmp_path)
    ds = generate_for_tenants(["tenant_a"], subscribers=80, days=40, fraud_rate=0.2, seed=7)
    frames = {e: getattr(ds, e) for e in ENTITIES}
    to_bronze(cfg, frames)
    silver = to_silver(cfg, frames)
    to_gold(cfg, silver=silver)        # persist Gold features to the (local) lake
    to_labels(cfg, dataset=ds)         # seed the labels store

    # LAKE path: no dataset/silver -> train reads Gold features + labels from storage
    result = train(cfg)
    assert "credit" in result["models"] and "fraud" in result["models"]
    assert storage.model_exists(storage.model_uri(cfg, "credit_model.joblib"), cfg)


def test_train_lake_path_errors_without_labels(tmp_path):
    import pytest
    from idp.pipeline import ENTITIES, to_bronze, to_gold, to_silver, train
    from idp.synthetic.generator import generate_for_tenants
    cfg = _local_cfg(tmp_path)
    ds = generate_for_tenants(["tenant_a"], subscribers=40, days=30, seed=7)
    frames = {e: getattr(ds, e) for e in ENTITIES}
    to_bronze(cfg, frames)
    to_gold(cfg, silver=to_silver(cfg, frames))   # Gold exists but NO labels seeded
    with pytest.raises(RuntimeError, match="labels"):
        train(cfg)
