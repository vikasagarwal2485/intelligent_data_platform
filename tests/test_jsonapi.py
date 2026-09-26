"""JSON:API source adapter (REQ-BRZ-04) against the real TM Forum data_samples."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from idp.ingestion import jsonapi

SAMPLES = Path(__file__).resolve().parents[1] / "data_samples"


def _load(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text())


def test_strip_uuid_wrapper():
    assert jsonapi.strip_uuid_wrapper("UUID(116100030984920447)") == "116100030984920447"
    assert jsonapi.strip_uuid_wrapper("plain-id") == "plain-id"
    assert jsonapi.strip_uuid_wrapper(None) is None


def test_normalize_money_splits_net_and_tax():
    block = {
        "tax-free-amount": {"amount": 5.40909, "currency": "EUR"},
        "tax-amounts": [{"tax-amount": {"amount": 0.540909, "currency": "EUR"}, "tax-rate": 10}],
    }
    money = jsonapi.normalize_money(block)
    assert money["net_amount"] == pytest.approx(5.40909, abs=1e-4)
    assert money["tax_amount"] == pytest.approx(0.540909, abs=1e-4)
    assert money["gross_amount"] == pytest.approx(5.949999, abs=1e-4)
    assert money["currency"] == "EUR"


def test_normalize_money_handles_empty():
    # SCN-BRZ-04.4 — no charged-amount block
    money = jsonapi.normalize_money(None)
    assert (money["net_amount"], money["tax_amount"], money["gross_amount"]) == (0.0, 0.0, 0.0)
    assert money["currency"] is None


def test_unwrap_data_usage_event_sample():
    # SCN-BRZ-04.1
    doc = _load("data_usage_events_v1.json")
    flat = jsonapi.unwrap_usage_event(doc)
    assert flat["service_id"] == "595984920447"
    assert flat["event_type"] == "data"
    assert flat["unit"] == "kilobytes"
    assert flat["qty"] == 657
    assert flat["gross_amount"] == pytest.approx(5.949999, abs=1e-4)
    # relationship UUID wrapper stripped
    assert flat["agreement_id"] == "116100030984920447"


def test_unwrap_voice_usage_event_sample():
    # SCN-BRZ-04.2
    doc = _load("voice_usage_events_v1.json")
    flat = jsonapi.unwrap_usage_event(doc)
    assert flat["event_type"] == "voice"
    assert flat["other_party_number"] == "595991222856"
    assert flat["traffic_case"] == "originating"
    assert flat["unit"] == "seconds"


def test_unwrap_recharge_event_sample():
    # SCN-BRZ-04.3
    doc = _load("recharge_events_v1.json")
    flat = jsonapi.unwrap_recharge_event(doc)
    assert flat["service_id"] == "595984920447"
    assert flat["amount"] == pytest.approx(20.0)
    assert flat["payment_method"] == "credit-card"
    assert flat["status"] == "successful"
    assert flat["agreement_id"] == "116100030984920447"
