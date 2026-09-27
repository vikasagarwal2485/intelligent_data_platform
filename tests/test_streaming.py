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


# --- landing zone -> Kafka (REQ-STR-01, `idp produce --from-landing`) -------------
def _small_config(tmp_path):
    import yaml

    from idp.common.config import DEFAULT_CONFIG_PATH
    raw = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["storage"] = {k: str(tmp_path / k) for k in
                      ("root", "landing", "bronze", "silver", "gold", "ml_artifacts")}
    raw["synthetic"].update({"subscribers_per_tenant": 8, "days": 4})
    path = tmp_path / "platform.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return str(path)


def _sent(producers):
    """Every message as (topic, key, serialized value), in send order."""
    out = []
    for p in producers:
        ser = p.kwargs["value_serializer"]
        out += [(t, k, ser(v)) for t, k, v in p.sent]
    return out


def _no_s3(monkeypatch):
    for var in ("IDP_S3_ENDPOINT", "IDP_SUBSCRIBERS", "IDP_DAYS"):
        monkeypatch.delenv(var, raising=False)


def test_landing_and_in_memory_production_send_the_same_messages(monkeypatch, tmp_path):
    # SCN-STR-01.3
    import sys
    import types

    from idp.cli import main
    _no_s3(monkeypatch)
    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaProducer=_FakeProducer))
    cfg = _small_config(tmp_path)

    _FakeProducer.instances = []
    assert main(["--config", cfg, "produce"]) == 0
    in_memory = _sent(_FakeProducer.instances)

    assert main(["--config", cfg, "generate"]) == 0
    _FakeProducer.instances = []
    assert main(["--config", cfg, "produce", "--from-landing"]) == 0
    from_landing = _sent(_FakeProducer.instances)

    assert in_memory and from_landing == in_memory


def test_rejected_landing_files_are_not_published_and_fail_the_run(monkeypatch, tmp_path, capsys):
    # SCN-STR-01.4 / SCN-STR-01.5
    import json
    import sys
    import types

    from idp.cli import main
    _no_s3(monkeypatch)
    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaProducer=_FakeProducer))
    cfg = _small_config(tmp_path)
    assert main(["--config", cfg, "generate"]) == 0
    land = tmp_path / "landing"
    bad = land / "tenant=tenant_a" / "recharge_events.json"
    records = json.loads(bad.read_text())
    records[0]["tenant_id"] = "tenant_b"                    # one foreign record poisons the file
    bad.write_text(json.dumps(records))
    (land / "tenant=tenant_x").mkdir()
    (land / "tenant=tenant_x" / "parties.json").write_text(json.dumps([{"tenant_id": "tenant_x", "id": "p"}]))

    _FakeProducer.instances = []
    assert main(["--config", cfg, "produce", "--from-landing"]) == 1
    topics = {t for t, _, _ in _sent(_FakeProducer.instances)}
    assert "t.tenant_a.recharge" not in topics                # nothing from the rejected file
    assert not any(t.startswith("t.tenant_x.") for t in topics)
    assert {"t.tenant_a.parties", "t.tenant_b.recharge"} <= topics   # the good files still go
    out = capsys.readouterr().out
    assert "REJECTED 2 landing file(s)" in out and "tenant mismatch" in out and "unknown tenant" in out


def test_empty_landing_zone_fails(monkeypatch, tmp_path, capsys):
    import sys
    import types

    from idp.cli import main
    _no_s3(monkeypatch)
    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaProducer=_FakeProducer))
    assert main(["--config", _small_config(tmp_path), "produce", "--from-landing"]) == 1
    assert "Run 'idp generate' first" in capsys.readouterr().out


class _Broker:
    """A loopback Kafka: producers append serialized bytes, consumers deserialize them."""
    topics: dict = {}

    class Producer:
        def __init__(self, **kw):
            self.ser = kw["value_serializer"]

        def send(self, topic, key=None, value=None):
            _Broker.topics.setdefault(topic, []).append(self.ser(value))

        def flush(self):
            pass

        def close(self):
            pass

    class Consumer:
        def __init__(self, *topics, **kw):
            de = kw["value_deserializer"]
            self.msgs = [_Msg(t, de(b)) for t in topics for b in _Broker.topics.get(t, [])]

        def __iter__(self):
            return iter(self.msgs)

        def commit(self):
            pass

        def close(self):
            pass


def test_landing_path_end_to_end_lands_exactly_the_landing_records(monkeypatch, tmp_path):
    # SCN-BRZ-03.4 — generate -> landing -> produce --from-landing -> Kafka -> consume -> Bronze
    import json
    import sys
    import types
    from collections import Counter

    from idp.cli import main
    from idp.common import catalog
    from idp.common.config import load_config
    _no_s3(monkeypatch)
    _Broker.topics = {}
    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(
        KafkaProducer=_Broker.Producer, KafkaConsumer=_Broker.Consumer))
    cfg_path = _small_config(tmp_path)
    assert main(["--config", cfg_path, "generate"]) == 0
    assert main(["--config", cfg_path, "produce", "--from-landing"]) == 0
    assert main(["--config", cfg_path, "consume", "--idle-timeout", "0.1"]) == 0

    landed = Counter()
    for f in (tmp_path / "landing").rglob("*.json"):
        landed[(f.parent.name.split("=", 1)[1], f.stem)] = len(json.loads(f.read_text()))
    cfg = load_config(cfg_path)
    bronze = Counter()
    for entity in {e for _, e in landed}:
        df = catalog.read_table(cfg, "bronze", entity)
        for tenant, n in df.groupby("tenant_id").size().items():
            bronze[(tenant, entity)] = n
    assert landed and bronze == landed
