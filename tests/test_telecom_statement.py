"""Telecom Statement 1.0 schemas (config/telecom-statement/1.0): the schemas are
valid JSON Schema 2020-12, every example validates, and the rules that carry the
standard's guarantees (minimisation, purpose binding, prepaid/postpaid blocks,
closed objects) reject what they should."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1] / "config" / "telecom-statement" / "1.0"
SCHEMAS = {p.name: json.loads(p.read_text()) for p in sorted(ROOT.glob("*.schema.json"))}
EXAMPLES = {p.name: json.loads(p.read_text()) for p in sorted((ROOT / "examples").glob("*.json"))}
SCHEMA_OF = {
    "credit-profile.prepaid.json": "credit-profile.schema.json",
    "credit-profile.postpaid.json": "credit-profile.schema.json",
    "fraud-signals.json": "fraud-signals.schema.json",
    "credit-monitoring.json": "credit-monitoring.schema.json",
}
REGISTRY = Registry().with_resources(
    (s["$id"], Resource.from_contents(s)) for s in SCHEMAS.values())


def errors(schema_name: str, doc: dict) -> list[str]:
    v = Draft202012Validator(SCHEMAS[schema_name], registry=REGISTRY, format_checker=FormatChecker())
    return [f"{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in v.iter_errors(doc)]


def example(name: str) -> dict:
    return copy.deepcopy(EXAMPLES[name])


def test_every_example_has_a_schema():
    assert set(EXAMPLES) == set(SCHEMA_OF)


@pytest.mark.parametrize("name", sorted(SCHEMAS))
def test_schemas_are_valid_2020_12(name):
    Draft202012Validator.check_schema(SCHEMAS[name])


@pytest.mark.parametrize("name", sorted(SCHEMA_OF))
def test_examples_validate(name):
    assert errors(SCHEMA_OF[name], example(name)) == []


@pytest.mark.parametrize("name", sorted(SCHEMA_OF))
def test_objects_are_closed(name):
    doc = example(name)
    doc["callDetailRecords"] = []
    assert errors(SCHEMA_OF[name], doc)
    doc = example(name)
    doc["body"]["location"] = {"cellId": "404-10-1234"}
    assert errors(SCHEMA_OF[name], doc)


def test_operator_extensions_are_namespaced():
    doc = example("credit-profile.prepaid.json")
    doc["extensions"] = {"x-op1-loyalty-tier": "GOLD"}
    assert errors("credit-profile.schema.json", doc) == []
    doc["extensions"] = {"loyaltyTier": "GOLD"}
    assert errors("credit-profile.schema.json", doc)


def test_msisdn_is_never_in_clear():
    doc = example("credit-profile.prepaid.json")
    doc["subject"]["msisdnMasked"] = "9876544321"
    assert errors("credit-profile.schema.json", doc)


def test_a_statement_validates_only_against_its_own_type():
    assert errors("credit-profile.schema.json", example("fraud-signals.json"))
    assert errors("fraud-signals.schema.json", example("credit-monitoring.json"))


def test_credit_profile_purpose_is_bound():
    doc = example("credit-profile.prepaid.json")
    doc["consent"]["dpvPurpose"] = "dpv:FraudPreventionAndDetection"
    assert errors("credit-profile.schema.json", doc)


def test_prepaid_has_no_postpaid_block_and_postpaid_no_recharges():
    prepaid = example("credit-profile.prepaid.json")
    prepaid["body"]["postpaid"] = example("credit-profile.postpaid.json")["body"]["postpaid"]
    assert errors("credit-profile.schema.json", prepaid)
    postpaid = example("credit-profile.postpaid.json")
    postpaid["body"]["recharges"] = example("credit-profile.prepaid.json")["body"]["recharges"]
    assert errors("credit-profile.schema.json", postpaid)


def test_invoice_paid_on_time_has_no_days_past_due():
    doc = example("credit-profile.postpaid.json")
    doc["body"]["postpaid"]["invoices"][0]["daysPastDue"] = 5
    assert errors("credit-profile.schema.json", doc)
    doc = example("credit-profile.postpaid.json")
    del doc["body"]["postpaid"]["invoices"][2]["paidInFullOn"]     # PAID_LATE needs its date
    assert errors("credit-profile.schema.json", doc)


def test_payment_channels_are_closed():
    doc = example("credit-profile.prepaid.json")
    doc["body"]["paymentMethods"]["channelMix"] = {"UPI_VPA_alice@bank": 1.0}
    assert errors("credit-profile.schema.json", doc)


def test_fraud_signals_are_short_lived():
    doc = example("fraud-signals.json")
    doc["consent"]["dataLife"] = "P30D"
    assert errors("fraud-signals.schema.json", doc)


def test_fraud_recency_is_banded():
    doc = example("fraud-signals.json")
    doc["body"]["simChange"]["latestWithin"] = "2026-10-04T09:40:00+05:30"
    assert errors("fraud-signals.schema.json", doc)


def test_kyc_match_returns_results_not_attributes():
    doc = example("fraud-signals.json")
    doc["body"]["kycMatch"]["givenName"] = "Asha"
    assert errors("fraud-signals.schema.json", doc)


def test_monitoring_is_periodic_with_frequency_and_expiry():
    doc = example("credit-monitoring.json")
    doc["consent"]["fetchType"] = "ONETIME"
    assert errors("credit-monitoring.schema.json", doc)
    doc = example("credit-monitoring.json")
    del doc["consent"]["frequency"]
    assert errors("credit-monitoring.schema.json", doc)


def test_monitoring_fetches_are_chained():
    doc = example("credit-monitoring.json")
    del doc["body"]["sequence"]["previousStatementId"]
    assert errors("credit-monitoring.schema.json", doc)
    doc["body"]["sequence"]["fetchNumber"] = 1
    assert errors("credit-monitoring.schema.json", doc) == []


def test_monitoring_events_are_closed_and_typed():
    doc = example("credit-monitoring.json")
    doc["body"]["events"].append({"type": "LOCATION_CHANGE", "date": "2026-09-30"})
    assert errors("credit-monitoring.schema.json", doc)
    doc = example("credit-monitoring.json")
    del doc["body"]["events"][1]["dpdBucket"]                      # BILL_OVERDUE detail
    assert errors("credit-monitoring.schema.json", doc)
    doc = example("credit-monitoring.json")
    doc["body"]["events"][4]["newIccid"] = "8991000000000000001"   # SIM_CHANGE carries no detail
    assert errors("credit-monitoring.schema.json", doc)


def test_prepaid_monitoring_reports_lapse_days():
    doc = example("credit-monitoring.json")
    doc["subject"]["connectionType"] = "PREPAID"
    doc["body"]["postpaid"] = "NOT_APPLICABLE"
    assert errors("credit-monitoring.schema.json", doc)            # rechargeLapseDays missing
    doc["body"]["indicators"]["rechargeLapseDays"] = 0
    assert errors("credit-monitoring.schema.json", doc) == []
