"""Silver's conform step: raw TMF Bronze -> canonical records (REQ-SLV-09/10)."""
from __future__ import annotations

import copy
import json

import pandas as pd
import pytest

from conftest import lake_cfg
from idp.common import catalog
from idp.common.records import to_records
from idp.ingestion.raw import event_row, raw_events, rows_frame
from idp.ingestion.tmf.events import to_event
from idp.ingestion.tmf.profile import load_profile
from idp.pipeline import to_bronze
from idp.synthetic.generator import ENTITIES, generate_for_tenants
from idp.transforms import conform, lake

TENANTS = ["tenant_a", "tenant_b"]


@pytest.fixture(scope="module")
def dataset():
    return generate_for_tenants(TENANTS, subscribers=10, days=2, fraud_rate=0.2, seed=21)


def _sorted(records):
    return sorted(to_records(pd.DataFrame(records)) if records else [], key=lambda r: r["id"])


def _conform_all(spark, cfg):
    """Every conformed entity as records, and every quarantine row."""
    frames, quarantine = conform.conform_reference(spark, cfg)
    out = {e: to_records(df.toPandas()) for e, df in frames.items() if df is not None}
    bad = [quarantine]
    for table, entities in conform.FACT_TABLES.items():
        raw = lake.read_table(spark, cfg, "bronze", table)
        if raw is None:
            continue
        ok, q, _ = conform.conform_facts(raw, table)
        pdf = ok.toPandas()
        for entity in entities:
            rows = pdf[pdf["_entity"] == entity].drop(columns="_entity")
            out[entity] = to_records(rows)
        bad.append(q.toPandas())
    return out, pd.concat(bad, ignore_index=True)


def _land(cfg, raw):
    to_bronze(cfg, raw)


def test_conformed_records_equal_the_generated_ones(spark, tmp_path, dataset):
    # SCN-SLV-09.1
    cfg = lake_cfg(tmp_path)
    _land(cfg, raw_events(dataset, load_profile("standard")))
    out, quarantine = _conform_all(spark, cfg)
    assert quarantine.empty
    for entity in ENTITIES:
        expected = sorted(to_records(getattr(dataset, entity)), key=lambda r: r["id"])
        assert sorted(out[entity], key=lambda r: r["id"]) == expected, entity


def _raw_row(resource, table, tenant="tenant_a", event_time=None, offset=0):
    suffix = {t: s for s, t in conform.bronze_tables().items()}[table]
    value = json.dumps(to_event(resource, tenant, event_time)).encode()
    return event_row(tenant, f"t.{tenant}.{suffix}", value, None, 0, offset)


@pytest.fixture(scope="module")
def tmf(dataset):
    from idp.streaming.producer import dataset_files
    return dict(dataset_files(dataset, load_profile("standard")))["tenant_a"]


def test_invalid_foreign_and_dangling_events_are_quarantined(spark, tmp_path, dataset, tmf):
    # SCN-SLV-09.2 / 09.3: none of them reaches the conformed records; each is
    # quarantined with its reason; the rest conforms
    cfg = lake_cfg(tmp_path)
    raw = raw_events(dataset, load_profile("standard"))
    usage = copy.deepcopy(tmf["productUsage.json"][0]); usage["id"] = "u-invalid"; usage["usageType"] = 42
    foreign = copy.deepcopy(tmf["productUsage.json"][1]); foreign["id"] = "u-foreign"; foreign["tenantId"] = "tenant_b"
    dangling = copy.deepcopy(tmf["agreement.json"][0]); dangling["id"] = "agr-dangling"
    dangling["engagedParty"][0]["partyOrPartyRole"]["id"] = "tenant_a-cust-gone"
    space = copy.deepcopy(tmf["agreement.json"][1]); space["id"] = "agr-space"
    next(c for c in space["characteristic"] if c["name"] == "businessLine")["value"] = "space"
    extra = {"tmf_product_usage": [_raw_row(usage, "tmf_product_usage", offset=10_000),
                                   _raw_row(foreign, "tmf_product_usage", offset=10_001)],
             "tmf_agreement": [_raw_row(dangling, "tmf_agreement", offset=10_000),
                               _raw_row(space, "tmf_agreement", offset=10_001)]}
    for table, rows in extra.items():
        raw[table] = pd.concat([raw[table], rows_frame(rows)], ignore_index=True)
    _land(cfg, raw)
    out, quarantine = _conform_all(spark, cfg)
    reasons = dict(zip(quarantine["resource_id"], quarantine["reason"]))
    assert set(reasons) == {"u-invalid", "u-foreign", "agr-dangling", "agr-space"}
    assert reasons["u-invalid"].startswith("invalid ProductUsage")
    assert "not the topic's tenant 'tenant_a'" in reasons["u-foreign"]
    assert "tenant_a-cust-gone" in reasons["agr-dangling"]
    assert reasons["agr-space"].startswith("breaks the canonical contract") and "BusinessLine" in reasons["agr-space"]
    assert "space" not in reasons["agr-space"].split(":", 1)[1].replace("BusinessLine", "")   # no values
    assert set(quarantine["source_table"]) == {"tmf_product_usage", "tmf_agreement"}
    ids = {r["id"] for e in out for r in out[e]}
    assert not ids & set(reasons)
    assert len(out["agreements"]) == len(dataset.agreements)                 # the rest conformed


def _land_party_versions(cfg, raw, tmf, order):
    party = tmf["individual.json"][0]
    newer = copy.deepcopy(party)
    newer["languageAbility"][0]["languageCode"] = "es-PY"
    early = _raw_row(party, "tmf_party", event_time="2026-02-01T00:00:00.000Z", offset=20_000)
    late = _raw_row(newer, "tmf_party", event_time="2026-03-01T00:00:00.000Z", offset=20_001)
    batches = [early, late] if order == "in order" else [late, early]
    to_bronze(cfg, raw)
    for row in batches:                              # separate appends: separate _ingested_at
        to_bronze(cfg, {"tmf_party": rows_frame([row])})
    return party["id"]


@pytest.mark.parametrize("order", ["in order", "reversed"])
def test_latest_version_wins(spark, tmp_path, dataset, tmf, order):
    # SCN-SLV-10.1 / 10.2: the later event_time wins, whatever order Bronze holds
    cfg = lake_cfg(tmp_path)
    base = raw_events(dataset, load_profile("standard"), event_time="2026-01-01T00:00:00.000Z")
    pid = _land_party_versions(cfg, base, tmf, order)
    frames, quarantine = conform.conform_reference(spark, cfg)
    assert quarantine.empty
    parties = frames["parties"].toPandas().set_index("id")
    assert parties.loc[pid, "language"] == "es-PY"
    assert parties.index.is_unique


def test_quarantine_through_silver_runs(spark, tmp_path, dataset, tmf):
    # SCN-SLV-06 (no duplicate quarantine rows on a re-run) and SCN-SLV-09.4 (after a
    # fix, a full refresh conforms the event and it leaves the quarantine). The fix
    # here is the missing customer arriving; a profile fix is reprocessed the same way.
    from idp.pipeline import to_silver
    cfg = lake_cfg(tmp_path)
    raw = raw_events(dataset, load_profile("standard"))
    orphan = copy.deepcopy(tmf["agreement.json"][0])
    orphan["id"] = "tenant_a-agr-orphan"
    orphan["engagedParty"][0]["partyOrPartyRole"]["id"] = "tenant_a-cust-late"
    next(c for c in orphan["characteristic"] if c["name"] == "msisdn")["value"] = "595900000001"   # its own subscriber
    bad_usage = copy.deepcopy(tmf["productUsage.json"][0]); bad_usage["id"] = "u-bad"; bad_usage["usageType"] = 42
    raw["tmf_agreement"] = pd.concat([raw["tmf_agreement"],
                                      rows_frame([_raw_row(orphan, "tmf_agreement", offset=30_000)])])
    raw["tmf_product_usage"] = pd.concat([raw["tmf_product_usage"],
                                          rows_frame([_raw_row(bad_usage, "tmf_product_usage", offset=30_000)])])
    _land(cfg, raw)
    first = to_silver(cfg)
    assert first["quarantined"] == {"tmf_agreement": 1, "tmf_product_usage": 1}
    q = catalog.read_table(cfg, "silver", "quarantine")
    assert set(q["resource_id"]) == {"tenant_a-agr-orphan", "u-bad"} and q["quarantined_at"].notna().all()
    from idp.common import contracts
    assert list(q.columns) == contracts.fields("silver/quarantine")      # no payload (SCN-STR-03.5)
    to_silver(cfg)                                              # re-run: no duplicates
    assert len(catalog.read_table(cfg, "silver", "quarantine")) == 2
    # the customer arrives; a full refresh conforms the agreement
    customer = copy.deepcopy(next(c for c in tmf["customer.json"]
                                  if c["id"] == tmf["agreement.json"][0]["engagedParty"][0]["partyOrPartyRole"]["id"]))
    customer["id"] = "tenant_a-cust-late"
    to_bronze(cfg, {"tmf_customer": rows_frame([_raw_row(customer, "tmf_customer", offset=30_000)])})
    to_silver(cfg, full_refresh=True)
    q = catalog.read_table(cfg, "silver", "quarantine")
    assert set(q["resource_id"]) == {"u-bad"}
    subs = catalog.read_table(cfg, "silver", "dim_subscriber")
    assert "tenant_a-cust-late" in set(subs["customer_id"])
