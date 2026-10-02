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


def test_silver_contract_matches_dim_subscriber(silver):
    # SCN-STR-03.4: Silver's dim_subscriber has exactly the documented fields
    assert list(silver["dim_subscriber"].columns) == contracts.fields("silver/dim_subscriber")
