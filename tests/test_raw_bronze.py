"""Raw Bronze: TMF events as received (REQ-BRZ-01, ingestion/raw.py)."""
from __future__ import annotations

import copy
import json

import pytest

from conftest import lake_cfg
from idp.common import catalog
from idp.ingestion.raw import RAW_COLUMNS, NotAnEvent, bronze_table, event_row, raw_events, rows_frame
from idp.ingestion.tmf.events import to_event
from idp.ingestion.tmf.profile import load_profile
from idp.pipeline import BRONZE_PARTITIONS, to_bronze
from idp.synthetic.generator import generate_for_tenants


@pytest.fixture(scope="module")
def dataset():
    return generate_for_tenants(["tenant_a", "tenant_b"], subscribers=8, days=2, fraud_rate=0.2, seed=5)


@pytest.fixture(scope="module")
def raw(dataset):
    return raw_events(dataset, load_profile("standard"))


def test_table_names():
    assert bronze_table("productUsage") == "tmf_product_usage"
    assert bronze_table("appliedCustomerBillingRate") == "tmf_applied_customer_billing_rate"
    assert bronze_table("party") == "tmf_party"


def test_raw_rows_hold_the_events_as_published(raw, dataset):
    assert set(raw) == {"tmf_customer", "tmf_party", "tmf_billing_account", "tmf_agreement", "tmf_product",
                        "tmf_applied_customer_billing_rate", "tmf_topup_balance", "tmf_product_usage"}
    usage = raw["tmf_product_usage"]
    assert list(usage.columns) == list(RAW_COLUMNS)
    assert len(usage) == len(dataset.voice_events) + len(dataset.data_events)
    first = usage.iloc[0]
    event = json.loads(first["payload"])
    assert (first["event_id"], first["event_type"], first["event_time"]) == \
        (event["eventId"], "ProductUsageCreateEvent", event["eventTime"])
    assert (first["resource_type"], first["resource_id"]) == ("IdpProductUsage", event["event"]["productUsage"]["id"])
    assert first["kafka_topic"] == f"t.{first['tenant_id']}.productUsage" and first["kafka_offset"] == 0
    assert set(raw["tmf_party"]["event_type"]) == {"IndividualCreateEvent", "OrganizationCreateEvent"}


def test_write_and_reload_bronze(tmp_path, raw):
    # SCN-BRZ-01.1 / 01.2: each topic's events go to bronze.tmf_<resource>, payload as
    # received, partitioned by (tenant_id, ingest_date)
    cfg = lake_cfg(tmp_path)
    landed = to_bronze(cfg, raw)
    assert set(landed["bronze_tables"]) == set(raw)
    back = catalog.read_table(cfg, "bronze", "tmf_customer").sort_values(["tenant_id", "kafka_offset"])
    sent = raw["tmf_customer"].sort_values(["tenant_id", "kafka_offset"])
    assert back["payload"].tolist() == sent["payload"].tolist()            # byte-identical
    assert {"_ingested_at", "ingest_date"} <= set(back.columns)
    spec = catalog.load_catalog(cfg).load_table("bronze.tmf_customer").spec()
    assert [f.name for f in spec.fields][:2] == list(BRONZE_PARTITIONS)


def test_append_only_history(tmp_path, raw):
    # SCN-BRZ-01.3: landing the same events twice keeps both batches as snapshots
    cfg = lake_cfg(tmp_path)
    batch = {"tmf_agreement": raw["tmf_agreement"]}
    to_bronze(cfg, batch)
    to_bronze(cfg, batch)
    table = catalog.load_catalog(cfg).load_table("bronze.tmf_agreement")
    assert len(table.snapshots()) == 2
    assert len(catalog.read_table(cfg, "bronze", "tmf_agreement")) == 2 * len(raw["tmf_agreement"])


def _row(resource, tenant="tenant_a", suffix="productUsage"):
    value = json.dumps(to_event(resource, tenant)).encode()
    return event_row(tenant, f"t.{tenant}.{suffix}", value, b"k", 0, 7)


def test_bronze_keeps_what_the_profile_doesnt_map(raw):
    # SCN-BRZ-01.4: an attribute the profile doesn't map stays in the payload
    usage = json.loads(raw["tmf_product_usage"].iloc[0]["payload"])["event"]["productUsage"]
    extra = copy.deepcopy(usage)
    extra["usageCharacteristic"].append({"@type": "StringCharacteristic", "name": "cellId",
                                         "valueType": "string", "value": "C-17"})
    row = _row(extra)
    stored = json.loads(row["payload"])["event"]["productUsage"]
    assert {"name": "cellId", "value": "C-17"}.items() <= stored["usageCharacteristic"][-1].items()
    assert stored["href"] == usage["href"]


def test_invalid_events_are_rows_not_rejections(raw):
    # SCN-BRZ-01.5: a resource failing its schema or another tenant's is still stored
    usage = json.loads(raw["tmf_product_usage"].iloc[0]["payload"])["event"]["productUsage"]
    broken = {**copy.deepcopy(usage), "usageType": 42, "tenantId": "tenant_b"}
    row = _row(broken)
    assert row["tenant_id"] == "tenant_a" and json.loads(row["payload"])["event"]["productUsage"]["usageType"] == 42
    assert len(rows_frame([row])) == 1


@pytest.mark.parametrize("value, reason", [
    (b"{not json", "invalid json"),
    (b"\xff\xfe", "invalid json"),
    (b"[1, 2]", "not a JSON object"),
    (b'{"eventId": "e"}', "missing eventType"),
    (b'{"eventType": "X", "eventId": "e", "event": {}}', "exactly one resource"),
    (b'{"eventType": "X", "eventId": "e", "event": {"a": {}, "b": {}}}', "exactly one resource"),
])
def test_what_cant_be_stored(value, reason):
    with pytest.raises(NotAnEvent, match=reason):
        event_row("tenant_a", "t.tenant_a.party", value, None, 0, 0)
