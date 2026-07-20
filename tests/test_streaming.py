"""Tests for the Kafka streaming bridge and the decoupled medallion stages.

The producer/consumer need a live Kafka, so here we test the parts that don't:
the topic<->table mapping and the independent Silver/Gold stage functions.
"""
from __future__ import annotations

from idp.common.config import load_config
from idp.pipeline import to_gold, to_scores, to_silver, train
from idp.streaming import SUFFIX_TO_TABLE, TABLE_TO_SUFFIX


def test_topic_table_mapping_roundtrip():
    for table, suffix in TABLE_TO_SUFFIX.items():
        assert SUFFIX_TO_TABLE[suffix] == table
    assert TABLE_TO_SUFFIX["voice_events"] == "usage.voice"
    assert TABLE_TO_SUFFIX["recharge_events"] == "recharge"


def test_silver_stage_from_bronze_frames(bronze_frames):
    """Silver stage builds canonical tables from Bronze frames."""
    cfg = load_config()
    silver = to_silver(cfg, bronze=bronze_frames, persist=False)
    assert set(silver) == {"dim_subscriber", "fact_usage", "fact_recharge", "fact_fee_charge"}
    assert len(silver["dim_subscriber"]) > 0
    assert set(silver["dim_subscriber"]["tenant_id"]) == {"tenant_a", "tenant_b"}


def test_gold_stage_builds_features(bronze_frames):
    """Gold stage builds feature tables from Silver (no models -> no scores)."""
    cfg = load_config()
    silver = to_silver(cfg, bronze=bronze_frames, persist=False)
    result = to_gold(cfg, silver=silver, persist=False)
    assert "credit_features" in result["gold_tables"]
    assert "fraud_features" in result["gold_tables"]


def test_score_stage_scores_gold_features(bronze_frames, multi_tenant_dataset):
    """The Score stage applies trained models to Gold features -> score tables."""
    cfg = load_config()
    silver = to_silver(cfg, bronze=bronze_frames, persist=False)
    gold = to_gold(cfg, silver=silver, persist=False)   # features only
    # train on the SAME dataset the silver was built from (labels must align)
    trained = train(cfg, dataset=multi_tenant_dataset, silver=silver, persist=False)
    result = to_scores(cfg, gold=gold, models=trained["models"], persist=False)
    assert "credit_scores" in result["score_tables"]
    assert "fraud_scores" in result["score_tables"]
