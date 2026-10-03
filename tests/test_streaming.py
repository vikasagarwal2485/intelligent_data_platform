"""Tests for the Kafka streaming bridge and the decoupled medallion stages.

No live Kafka: the independent Silver/Gold stage functions, and the producer and
consumer against an in-memory broker (tests/kafka_fake.py). Kafka carries TM Forum
create events (REQ-STR-01); the consumer adapts them to canonical Bronze (REQ-STR-02).
"""
from __future__ import annotations

import copy
import json

import pytest

from idp.pipeline import to_scores


def test_silver_stage_from_bronze(silver):
    """Silver stage builds the canonical tables from the landed Bronze (conftest `lake`)."""
    assert set(silver) == {"dim_subscriber", "dim_customer", "dim_billing_account", "dim_product",
                           "fact_usage", "fact_recharge", "fact_fee_charge"}
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



# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #
def _broker(monkeypatch):
    import sys

    from kafka_fake import FakeBroker
    broker = FakeBroker()
    monkeypatch.setitem(sys.modules, "kafka", broker.module())
    return broker


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


def _no_s3(monkeypatch):
    for var in ("IDP_S3_ENDPOINT", "IDP_SUBSCRIBERS", "IDP_DAYS", "IDP_TMF_PROFILE"):
        monkeypatch.delenv(var, raising=False)


def _messages(broker):
    """Every message as (topic, key, value without eventTime), in topic order."""
    out = []
    for topic in sorted(broker.log):
        for m in broker.log[topic]:
            value = json.loads(m.value)
            value.pop("eventTime", None)
            out.append((topic, m.key, value))
    return out


def _canonical(df):
    """Records sorted by id, without Bronze's landing columns."""
    from idp.common.records import to_records
    from idp.transforms.incremental import LANDING_COLUMNS
    recs = to_records(df.drop(columns=[c for c in LANDING_COLUMNS if c in df.columns]))
    return sorted(recs, key=lambda r: r["id"])


@pytest.fixture(scope="module")
def dataset():
    from idp.synthetic.generator import generate_for_tenants
    return generate_for_tenants(["tenant_a", "tenant_b"], subscribers=12, days=3, fraud_rate=0.2, seed=9)


@pytest.fixture(scope="module")
def profile():
    from idp.ingestion.tmf.profile import load_profile
    return load_profile("standard")


# --------------------------------------------------------------------------- #
# producer: TMF create events (REQ-STR-01)                                     #
# --------------------------------------------------------------------------- #
def test_one_topic_per_tmf_resource():
    from idp.streaming import RESOURCE_TOPIC, TOPIC_RESOURCES
    assert list(TOPIC_RESOURCES)[0] == "customer"                     # published first
    assert RESOURCE_TOPIC["Individual"] == RESOURCE_TOPIC["Organization"] == "party"
    assert RESOURCE_TOPIC["ProductUsage"] == "productUsage"
    assert RESOURCE_TOPIC["AppliedCustomerBillingRate"] == "appliedCustomerBillingRate"
    from idp.ingestion.tmf.profile import load_profile
    prof = load_profile("standard")
    assert {r.resource for r in prof.resources} | {prof.customer.resource} == set(RESOURCE_TOPIC)


def test_produce_publishes_create_events_per_resource_topic(monkeypatch, dataset, profile):
    # SCN-STR-01.1 / 01.2 / 01.7
    from idp.streaming import produce_to_kafka
    broker = _broker(monkeypatch)
    counts = produce_to_kafka(dataset, "broker:9092", profile=profile)

    voice = [e for e in broker.values("t.tenant_a.productUsage")
             if e["event"]["productUsage"]["usageType"] == "voice"]
    assert len(voice) == (dataset.voice_events["tenant_id"] == "tenant_a").sum()
    assert {e["eventType"] for e in voice} == {"ProductUsageCreateEvent"}
    fees = broker.values("t.tenant_b.appliedCustomerBillingRate")
    assert len(fees) == (dataset.fee_charges["tenant_id"] == "tenant_b").sum()
    party = broker.values("t.tenant_a.party")
    assert {e["eventType"] for e in party} == {"IndividualCreateEvent", "OrganizationCreateEvent"}
    assert all(e["event"][e["eventType"][0].lower() + e["eventType"][1:-len("CreateEvent")]]["tenantId"]
               == "tenant_a" for e in party)
    assert not any(t.split(".")[2] in ("parties", "usage") for t in counts)     # no old topics
    assert sum(counts.values()) == sum(len(v) for v in broker.log.values())


def test_customers_first_keys_are_natural(monkeypatch, dataset, profile):
    # customers are on the broker (flushed) before anything referencing them; keys
    # are the subscriber's MSISDN where the resource has one, else the id
    from idp.streaming import produce_to_kafka
    broker = _broker(monkeypatch)
    produce_to_kafka(dataset, "broker:9092", profile=profile)
    n_customers = len(broker.log["t.tenant_a.customer"])
    assert broker.flushes[0] == n_customers                       # flush right after tenant_a's customers
    usage = broker.log["t.tenant_a.productUsage"][0]
    usage_res = json.loads(usage.value)["event"]["productUsage"]
    msisdn = next(c["value"] for c in usage_res["usageCharacteristic"] if c["name"] == "msisdn")
    assert usage.key == msisdn.encode()
    agreement = broker.log["t.tenant_a.agreement"][0]
    assert agreement.key == dataset.agreements.set_index("id").loc[
        json.loads(agreement.value)["event"]["agreement"]["id"], "msisdn"].encode()
    for topic in ("t.tenant_a.customer", "t.tenant_a.party", "t.tenant_a.product"):
        m = broker.log[topic][0]
        assert m.key == json.loads(m.value)["event"][list(json.loads(m.value)["event"])[0]]["id"].encode()


def test_landing_and_in_memory_production_send_the_same_messages(monkeypatch, tmp_path):
    # SCN-STR-01.3: equal apart from eventTime
    from idp.cli import main
    _no_s3(monkeypatch)
    cfg = _small_config(tmp_path)
    broker = _broker(monkeypatch)
    assert main(["--config", cfg, "produce"]) == 0
    in_memory = _messages(broker)

    assert main(["--config", cfg, "generate"]) == 0
    broker = _broker(monkeypatch)
    assert main(["--config", cfg, "produce", "--from-landing"]) == 0
    assert in_memory and _messages(broker) == in_memory


def test_rejected_landing_files_are_not_published_and_fail_the_run(monkeypatch, tmp_path, capsys):
    # SCN-STR-01.4 / 01.5 / 01.6, and a rejected file's dependents
    from idp.cli import main
    _no_s3(monkeypatch)
    cfg = _small_config(tmp_path)
    assert main(["--config", cfg, "generate"]) == 0
    land = tmp_path / "landing"

    def edit(path, change):
        resources = json.loads(path.read_text())
        change(resources)
        path.write_text(json.dumps(resources))

    def foreign(rs):
        rs[0]["tenantId"] = "tenant_b"                      # one foreign resource poisons the file

    def dangling(rs):
        rs[0]["engagedParty"][0]["partyOrPartyRole"]["id"] = "tenant_a-cust-gone"

    def invalid(rs):
        rs[0]["status"] = 42                                 # schema violation

    edit(land / "tenant=tenant_a" / "topupBalance.json", foreign)
    edit(land / "tenant=tenant_a" / "agreement.json", dangling)
    edit(land / "tenant=tenant_b" / "customer.json", invalid)
    (land / "tenant=tenant_x").mkdir()
    (land / "tenant=tenant_x" / "individual.json").write_text(json.dumps([]))

    broker = _broker(monkeypatch)
    assert main(["--config", cfg, "produce", "--from-landing"]) == 1
    topics = broker.topics()
    for gone in ("t.tenant_a.topupBalance", "t.tenant_a.agreement",       # own reasons
                 "t.tenant_b.customer",
                 "t.tenant_b.agreement"):                                  # reads its customer
        assert gone not in topics, gone
    assert not any(t.startswith("t.tenant_x.") for t in topics)
    assert {"t.tenant_a.customer", "t.tenant_a.party", "t.tenant_a.productUsage",
            "t.tenant_b.party", "t.tenant_b.billingAccount",              # payer: no customer needed
            "t.tenant_b.topupBalance", "t.tenant_b.productUsage"} <= topics      # the rest goes
    out = capsys.readouterr().out
    assert "tenant mismatch" in out and "unknown tenant" in out and "tenant_a-cust-gone" in out
    assert "depends on rejected file 'customer.json'" in out


def test_empty_landing_zone_fails(monkeypatch, tmp_path, capsys):
    from idp.cli import main
    _no_s3(monkeypatch)
    _broker(monkeypatch)
    assert main(["--config", _small_config(tmp_path), "produce", "--from-landing"]) == 1
    assert "Run 'idp generate' first" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# consumer: TMF events -> raw Bronze (REQ-STR-04)                              #
# --------------------------------------------------------------------------- #
def _published(broker):
    """Every message value per raw Bronze table, as published."""
    from idp.ingestion.raw import bronze_table
    out: dict[str, list[bytes]] = {}
    for topic, msgs in broker.log.items():
        suffix = topic.split(".", 2)[2]
        if suffix != "dlq":
            out.setdefault(bronze_table(suffix), []).extend(m.value for m in msgs)
    return out


def test_consume_lands_every_event_raw(monkeypatch, tmp_path, dataset, profile):
    # SCN-STR-04.1 / 04.3: each published event once, customers included, payload
    # equal to the message value; no Silver; offsets committed after landing
    from conftest import lake_cfg
    from idp.common import catalog
    from idp.pipeline import to_bronze
    from idp.streaming import produce_to_kafka
    from idp.streaming.consumer import consume_to_bronze
    broker = _broker(monkeypatch)
    produce_to_kafka(dataset, "x:9092", profile=profile)
    cfg = lake_cfg(tmp_path)
    result = consume_to_bronze("x:9092", ["tenant_a", "tenant_b"], land=lambda f: to_bronze(cfg, f))
    assert result.dead_letters == [] and broker.commits == 1
    published = _published(broker)
    assert "tmf_customer" in published
    for table, values in published.items():
        landed = catalog.read_table(cfg, "bronze", table)
        assert sorted(landed["payload"]) == sorted(v.decode() for v in values), table
    assert not catalog.table_exists(cfg, "silver", "dim_subscriber")
    again = consume_to_bronze("x:9092", ["tenant_a", "tenant_b"], land=lambda f: to_bronze(cfg, f))
    assert again.frames == {} and again.dead_letters == []


def test_customer_from_an_earlier_run_is_landed(monkeypatch, dataset, profile):
    # SCN-STR-04.4: the consumer doesn't resolve: billing accounts, agreements and
    # parties consumed after their customers are landed as they are
    from idp.streaming.consumer import consume_to_bronze
    from idp.streaming.producer import dataset_files, tenant_events
    broker = _broker(monkeypatch)
    events = list(tenant_events(profile, "tenant_a", dict(dataset_files(dataset, profile))["tenant_a"]))
    landed = []
    for wanted in (("customer",), ("party", "billingAccount", "agreement")):
        for suffix, key, event in events:
            if suffix in wanted:
                broker.append(f"t.tenant_a.{suffix}", event, key)
        result = consume_to_bronze("x:9092", ["tenant_a"], land=landed.append)
        assert result.dead_letters == []
    assert set(landed[0]) == {"tmf_customer"}
    assert set(landed[1]) == {"tmf_party", "tmf_billing_account", "tmf_agreement"}
    assert len(landed[1]["tmf_agreement"]) == int((dataset.agreements["tenant_id"] == "tenant_a").sum())


def _bad_messages(broker, dataset, profile):
    """Publish the dataset, a message that isn't JSON, and two events Silver must
    quarantine (a dangling customer, a contract violation) but Bronze must keep."""
    from idp.ingestion.tmf.events import to_event
    from idp.streaming import produce_to_kafka
    from idp.streaming.producer import dataset_files
    produce_to_kafka(dataset, "x:9092", profile=profile)
    agreement = dict(dataset_files(dataset, profile))["tenant_a"]["agreement.json"][0]
    dangling = copy.deepcopy(agreement)
    dangling["id"] = "tenant_a-agr-dangling"
    dangling["engagedParty"][0]["partyOrPartyRole"]["id"] = "tenant_a-cust-gone"
    broker.append("t.tenant_a.agreement", to_event(dangling, "tenant_a"), b"k1")
    off_contract = copy.deepcopy(agreement)
    off_contract["id"] = "tenant_a-agr-space"
    next(c for c in off_contract["characteristic"] if c["name"] == "businessLine")["value"] = "space"
    broker.append("t.tenant_a.agreement", to_event(off_contract, "tenant_a"), b"k2")
    broker.append("t.tenant_a.productUsage", b"{not json", b"k3")


def test_only_unstorable_messages_are_dead_lettered(monkeypatch, tmp_path, dataset, profile):
    # SCN-STR-04.5 / 04.6: the message that isn't JSON goes to the DLQ with its
    # reason and source position; the dangling and off-contract events are landed
    from conftest import lake_cfg
    from idp.common import catalog
    from idp.pipeline import to_bronze
    from idp.streaming.consumer import consume_to_bronze
    broker = _broker(monkeypatch)
    _bad_messages(broker, dataset, profile)
    cfg = lake_cfg(tmp_path)
    result = consume_to_bronze("x:9092", ["tenant_a", "tenant_b"], land=lambda f: to_bronze(cfg, f))
    assert [(d.key, d.reason.split(":")[0]) for d in result.dead_letters] == [(b"k3", "invalid json")]
    [dlq] = broker.log["t.tenant_a.dlq"]
    headers = dict(dlq.headers)
    assert dlq.value == b"{not json" and headers["idp.source.topic"] == b"t.tenant_a.productUsage"
    assert int(headers["idp.source.offset"]) == len(broker.log["t.tenant_a.productUsage"]) - 1
    agreements = catalog.read_table(cfg, "bronze", "tmf_agreement")
    assert {"tenant_a-agr-dangling", "tenant_a-agr-space"} <= set(agreements["resource_id"])
    assert broker.commits == 1


def test_consume_fails_when_messages_were_dead_lettered(monkeypatch, tmp_path, dataset, profile, capsys):
    from idp.cli import main
    _no_s3(monkeypatch)
    broker = _broker(monkeypatch)
    _bad_messages(broker, dataset, profile)
    assert main(["--config", _small_config(tmp_path), "consume", "--idle-timeout", "0.1"]) == 1
    assert "DEAD-LETTERED 1 message(s)" in capsys.readouterr().out


def test_failed_landing_commits_nothing(monkeypatch, dataset, profile):
    # nothing landed -> nothing dead-lettered or committed; the next run re-reads
    from idp.streaming import produce_to_kafka
    from idp.streaming.consumer import consume_to_bronze
    broker = _broker(monkeypatch)
    produce_to_kafka(dataset, "x:9092", profile=profile)
    broker.append("t.tenant_a.party", b"{not json")

    def boom(_frames):
        raise RuntimeError("catalog down")
    with pytest.raises(RuntimeError):
        consume_to_bronze("x:9092", ["tenant_a"], land=boom)
    assert broker.commits == 0 and "t.tenant_a.dlq" not in broker.log
    landed = []
    consume_to_bronze("x:9092", ["tenant_a"], land=landed.append)
    assert landed and broker.commits == 1 and len(broker.log["t.tenant_a.dlq"]) == 1


def test_empty_topics_no_snapshot(monkeypatch, tmp_path):
    # SCN-STR-04.2: nothing consumed, no landing, no Bronze snapshot, nothing committed
    from conftest import lake_cfg
    from idp.common import catalog
    from idp.pipeline import to_bronze
    from idp.streaming.consumer import consume_to_bronze
    broker = _broker(monkeypatch)
    cfg = lake_cfg(tmp_path)
    out = consume_to_bronze("x:9092", ["tenant_a"], land=lambda f: to_bronze(cfg, f))
    assert out.frames == {} and out.dead_letters == [] and broker.commits == 0
    assert catalog.table_exists(cfg, "bronze", "tmf_party") is False


def test_landing_path_end_to_end_conforms_exactly_the_generated_records(spark, monkeypatch, tmp_path):
    # SCN-BRZ-03.4: generate -> landing -> produce --from-landing -> Kafka (TMF) ->
    # consume -> raw Bronze -> Silver conform: one raw event per landed resource, and
    # the conformed records equal the generated canonical records
    from conftest import lake_cfg
    from idp.cli import main
    from idp.common.config import load_config
    from idp.common.records import to_records
    from idp.pipeline import ENTITIES, _gen_dataset
    from idp.transforms import conform, lake
    _no_s3(monkeypatch)
    broker = _broker(monkeypatch)
    cfg_path = _small_config(tmp_path)
    assert main(["--config", cfg_path, "generate"]) == 0
    assert main(["--config", cfg_path, "produce", "--from-landing"]) == 0
    assert main(["--config", cfg_path, "consume", "--idle-timeout", "0.1"]) == 0
    cfg = load_config(cfg_path)
    published = sum(len(m) for t, m in broker.log.items() if not t.endswith(".dlq"))
    landed = sum(len(catalog_rows) for catalog_rows in
                 (lake.read_table(spark, cfg, "bronze", t).collect() for t in conform.bronze_tables().values()))
    assert landed == published

    frames, quarantine = conform.conform_reference(spark, cfg)
    assert quarantine.empty
    conformed = {e: to_records(df.toPandas()) for e, df in frames.items() if df is not None}
    for table, entities in conform.FACT_TABLES.items():
        ok, bad, _ = conform.conform_facts(lake.read_table(spark, cfg, "bronze", table), table)
        assert bad.count() == 0
        pdf = ok.toPandas()
        for entity in entities:
            conformed[entity] = to_records(pdf[pdf["_entity"] == entity].drop(columns="_entity"))
    generated = _gen_dataset(cfg)
    for entity in ENTITIES:
        key = lambda r: r["id"]
        assert sorted(conformed[entity], key=key) == sorted(to_records(getattr(generated, entity)), key=key), entity
