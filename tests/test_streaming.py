"""Tests for the Kafka streaming bridge and the Bronze->Silver->Gold processor.

The producer/consumer themselves need a live Kafka, so here we test the parts
that don't: the topic<->table mapping and the process_bronze medallion step.
"""
from __future__ import annotations

from idp.common.config import load_config
from idp.pipeline import process_bronze
from idp.streaming import SUFFIX_TO_TABLE, TABLE_TO_SUFFIX


def test_topic_table_mapping_roundtrip():
    for table, suffix in TABLE_TO_SUFFIX.items():
        assert SUFFIX_TO_TABLE[suffix] == table
    # the event topics use dotted suffixes
    assert TABLE_TO_SUFFIX["voice_events"] == "usage.voice"
    assert TABLE_TO_SUFFIX["recharge_events"] == "recharge"


def test_process_bronze_builds_silver_and_gold(bronze_frames):
    cfg = load_config()
    summary = process_bronze(cfg, bronze_frames, persist=False)
    assert summary["counts"]["subscribers"] > 0
    assert "credit_features" in summary["gold_tables"]
    assert "fraud_features" in summary["gold_tables"]
    # tenant isolation preserved through the streaming path
    assert set(summary["silver"]["dim_subscriber"]["tenant_id"]) == {"tenant_a", "tenant_b"}


def test_process_bronze_scores_when_models_exist(bronze_frames, tmp_path):
    """If trained models are present, the processor also emits scores."""
    from dataclasses import replace
    from idp.common.config import StorageConfig
    from idp.pipeline import run

    cfg = load_config()
    cfg.raw["synthetic"].update({"subscribers_per_tenant": 120, "days": 30, "fraud_rate": 0.15})
    cfg.storage = StorageConfig(
        root=str(tmp_path), landing=str(tmp_path / "l"), bronze=str(tmp_path / "b"),
        silver=str(tmp_path / "s"), gold=str(tmp_path / "g"), ml_artifacts=str(tmp_path / "ml"),
    )
    run(cfg, persist=True)  # trains + saves models to tmp_path/ml
    summary = process_bronze(cfg, bronze_frames, persist=False)
    assert "credit_scores" in summary["gold_tables"]
    assert "fraud_scores" in summary["gold_tables"]
