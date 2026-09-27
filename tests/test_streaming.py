"""Tests for the Kafka streaming bridge and the decoupled medallion stages.

No live Kafka: the topic<->table mapping, the independent Silver/Gold stage
functions, and the producer/consumer entry points against fake Kafka clients.
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
    from idp.common import catalog
    gold = {name for _, name in catalog.load_catalog(lake).list_tables("gold")}
    assert {"credit_features", "fraud_features"} <= gold and "credit_scores" not in gold


def test_score_stage_scores_gold_features(tmp_path, lake, credit_labeled, fraud_labeled):
    """The Score stage applies trained models to the Gold features read from the lake."""
    from conftest import clone_lake
    from idp.ml.credit_model import CreditLimitModel
    from idp.ml.fraud_model import FraudModel
    cfg = clone_lake(lake, tmp_path / "lake", layers=("gold",), tables={"credit_features", "fraud_features"})
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


# --- Kafka -> Bronze commit semantics (REQ-STR-02) -----------------------------
class _Msg:
    def __init__(self, topic, value):
        self.topic, self.value = topic, value


class _FakeConsumer:
    instances: list = []

    def __init__(self, *topics, **kwargs):
        self.kwargs, self.committed, self.closed = kwargs, False, False
        self.msgs = [_Msg("t.tenant_a.parties", {"tenant_id": "tenant_a", "id": "p1"})] if _FakeConsumer.with_data else []
        _FakeConsumer.instances.append(self)

    def __iter__(self):
        return iter(self.msgs)

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def _fake_kafka(monkeypatch, with_data):
    import sys, types
    _FakeConsumer.instances, _FakeConsumer.with_data = [], with_data
    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaConsumer=_FakeConsumer))


def test_offsets_committed_only_after_landing(monkeypatch):
    from idp.streaming.consumer import consume_to_bronze
    _fake_kafka(monkeypatch, with_data=True)
    landed = []
    consume_to_bronze("x:9092", ["tenant_a"], land=landed.append)
    c = _FakeConsumer.instances[0]
    assert c.kwargs["enable_auto_commit"] is False
    assert len(landed) == 1 and c.committed and c.closed


def test_failed_landing_commits_nothing(monkeypatch):
    import pytest
    from idp.streaming.consumer import consume_to_bronze
    _fake_kafka(monkeypatch, with_data=True)

    def boom(_frames):
        raise RuntimeError("catalog down")
    with pytest.raises(RuntimeError):
        consume_to_bronze("x:9092", ["tenant_a"], land=boom)
    c = _FakeConsumer.instances[0]
    assert not c.committed and c.closed


def test_empty_topics_no_snapshot(monkeypatch, tmp_path):
    # SCN-STR-02.2 — nothing consumed: no landing, no Bronze snapshot
    from conftest import lake_cfg
    from idp.common import catalog
    from idp.pipeline import to_bronze
    from idp.streaming.consumer import consume_to_bronze
    _fake_kafka(monkeypatch, with_data=False)
    cfg = lake_cfg(tmp_path)
    out = consume_to_bronze("x:9092", ["tenant_a"], land=lambda f: to_bronze(cfg, f))
    assert out == {} and not _FakeConsumer.instances[0].committed
    assert catalog.table_exists(cfg, "bronze", "parties") is False


def test_consume_appends_to_iceberg_bronze(monkeypatch, tmp_path):
    # SCN-STR-02.1 — consumed records become a Bronze snapshot; no Silver/Gold
    from conftest import lake_cfg
    from idp.common import catalog
    from idp.pipeline import to_bronze
    from idp.streaming.consumer import consume_to_bronze
    _fake_kafka(monkeypatch, with_data=True)
    cfg = lake_cfg(tmp_path)
    consume_to_bronze("x:9092", ["tenant_a"], land=lambda f: to_bronze(cfg, f))
    assert catalog.read_table(cfg, "bronze", "parties")["id"].tolist() == ["p1"]
    assert not catalog.table_exists(cfg, "silver", "dim_subscriber")


# --- synthetic data -> Kafka (REQ-STR-01) -----------------------------------------
class _FakeProducer:
    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs, self.sent, self.flushed, self.closed = kwargs, [], False, False
        _FakeProducer.instances.append(self)

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def test_produce_to_kafka_publishes_per_tenant_topics_with_natural_keys(monkeypatch):
    # SCN-STR-01.1 / 01.2 through the entry point itself, with no broker
    import sys
    import types

    import pandas as pd

    from idp.streaming import produce_to_kafka
    _FakeProducer.instances = []
    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaProducer=_FakeProducer))
    frames = {t: pd.DataFrame() for t in TABLE_TO_SUFFIX}
    frames["voice_events"] = pd.DataFrame({"tenant_id": ["tenant_a", "tenant_a"], "id": ["v1", "v2"],
                                           "service_id": ["s1", "s2"]})
    frames["fee_charges"] = pd.DataFrame({"tenant_id": ["tenant_b"], "id": ["f1"]})

    counts = produce_to_kafka(types.SimpleNamespace(**frames), "broker:9092")

    assert counts == {"t.tenant_a.usage.voice": 2, "t.tenant_b.fees": 1}
    [producer] = _FakeProducer.instances
    assert producer.kwargs["bootstrap_servers"] == "broker:9092"
    assert [(t, k) for t, k, _ in producer.sent] == [
        ("t.tenant_a.usage.voice", b"s1"), ("t.tenant_a.usage.voice", b"s2"),   # subscriber key
        ("t.tenant_b.fees", b"f1"),                                           # else the record id
    ]
    assert producer.flushed and producer.closed
