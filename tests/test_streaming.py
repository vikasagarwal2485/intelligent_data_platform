"""Tests for the Kafka streaming bridge and the decoupled medallion stages.

The producer/consumer need a live Kafka, so here we test the parts that don't:
the topic<->table mapping and the independent Silver/Gold stage functions.
"""
from __future__ import annotations

from pathlib import Path

from idp.pipeline import to_scores
from idp.streaming import SUFFIX_TO_TABLE, TABLE_TO_SUFFIX


def test_topic_table_mapping_roundtrip():
    for table, suffix in TABLE_TO_SUFFIX.items():
        assert SUFFIX_TO_TABLE[suffix] == table
    assert TABLE_TO_SUFFIX["voice_events"] == "usage.voice"
    assert TABLE_TO_SUFFIX["recharge_events"] == "recharge"


def test_silver_stage_from_bronze(silver):
    """Silver stage builds the canonical tables from the landed Bronze (conftest `lake`)."""
    assert set(silver) == {"dim_subscriber", "fact_usage", "fact_recharge", "fact_fee_charge"}
    assert len(silver["dim_subscriber"]) > 0
    assert set(silver["dim_subscriber"]["tenant_id"]) == {"tenant_a", "tenant_b"}


def test_gold_stage_builds_features(lake):
    """Gold stage builds feature tables only from the persisted Silver (no scores)."""
    gold = Path(lake.storage.gold)
    assert (gold / "credit_features").is_dir() and (gold / "fraud_features").is_dir()
    assert not (gold / "credit_scores").exists()


def test_score_stage_scores_gold_features(tmp_path, lake, credit_labeled, fraud_labeled):
    """The Score stage applies trained models to the Gold features read from the lake."""
    import shutil

    from idp.ml.credit_model import CreditLimitModel
    from idp.ml.fraud_model import FraudModel
    from conftest import lake_cfg
    cfg = lake_cfg(tmp_path)
    for table in ("credit_features", "fraud_features"):
        shutil.copytree(Path(lake.storage.gold) / table, Path(cfg.storage.gold) / table)
    models = {"credit": CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"]),
              "fraud": FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])}
    result = to_scores(cfg, models=models, persist=False)
    assert "credit_scores" in result["score_tables"]
    assert "fraud_scores" in result["score_tables"]


def test_fee_charges_have_their_own_topic():
    # SCN-STR-01.2 — fee charges land on t.<tenant>.fees
    from idp.streaming.producer import _topic
    assert TABLE_TO_SUFFIX["fee_charges"] == "fees"
    assert _topic("t", "tenant_b", TABLE_TO_SUFFIX["fee_charges"]) == "t.tenant_b.fees"
