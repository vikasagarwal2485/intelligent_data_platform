"""Canonical record contracts in config/avro (REQ-STR-03)."""
from __future__ import annotations

import pytest

from idp.common import contracts
from idp.common.records import to_records
from idp.streaming import BRONZE_ENTITIES


def test_every_bronze_entity_has_a_contract():
    assert set(contracts.ENTITY_SCHEMA) == set(BRONZE_ENTITIES)


@pytest.mark.parametrize("entity", sorted(contracts.ENTITY_SCHEMA))
def test_contract_matches_the_canonical_records(entity, multi_tenant_dataset):
    # SCN-STR-03.3: the contract's fields are exactly the entity's canonical
    # columns, in order, and every generated record conforms
    recs = to_records(getattr(multi_tenant_dataset, entity))
    assert recs, entity
    assert list(recs[0]) == contracts.fields(contracts.ENTITY_SCHEMA[entity])
    bad = [(r["id"], err) for r in recs if (err := contracts.error(entity, r))]
    assert bad == []


def test_contract_violations_are_reported(multi_tenant_dataset):
    rec = to_records(multi_tenant_dataset.recharge_events)[0]
    assert "RechargeStatus" in contracts.error("recharge_events", {**rec, "status": "completed"})
    assert "amount" in contracts.error("recharge_events", {**rec, "amount": None})
    assert "unexpected fields ['gross_amount']" in contracts.error(
        "recharge_events", {**rec, "gross_amount": 1.0})
    usage = to_records(multi_tenant_dataset.data_events)[0]
    assert contracts.error("data_events", {**usage, "network_tech": "4G+"}) is None   # open domain


def test_contract_reasons_never_carry_values(multi_tenant_dataset):
    # quarantine reasons are stored in Silver, which must hold no PII (REQ-SLV-03):
    # a broken MSISDN or email is named, its value isn't
    agreement = to_records(multi_tenant_dataset.agreements)[0]
    reason = contracts.error("agreements", {**agreement, "msisdn": 595912345678, "business_line": "space"})
    assert "msisdn: expected a string" in reason and "business_line: expected one of BusinessLine" in reason
    assert "595912345678" not in reason and "space" not in reason
    party = to_records(multi_tenant_dataset.parties)[0]
    reason = contracts.error("parties", {**party, "email": None})
    assert reason == "party: email: missing, expected a string"


def test_spark_schema_follows_the_contract():
    schema = contracts.spark_schema("silver/dim_customer")
    assert [f.name for f in schema.fields] == contracts.fields("silver/dim_customer")
    types = {f.name: (f.dataType.simpleString(), f.nullable) for f in schema.fields}
    assert types["customer_id"] == ("string", False)
    assert types["segment"] == ("string", True)                 # nullable enum
    assert types["valid_from"] == ("timestamp", True)
    assert contracts.spark_schema("usage_event")["qty"].dataType.simpleString() == "bigint"


def test_silver_contract_matches_dim_subscriber(silver):
    # SCN-STR-03.4: Silver's dim_subscriber has exactly the documented fields
    assert list(silver["dim_subscriber"].columns) == contracts.fields("silver/dim_subscriber")


@pytest.mark.parametrize("table", ["dim_customer", "dim_billing_account", "dim_product"])
def test_silver_contracts_match_the_new_dimensions(silver, table):
    # SCN-STR-03.5
    assert list(silver[table].columns) == contracts.fields(f"silver/{table}")

