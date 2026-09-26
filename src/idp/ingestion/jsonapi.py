"""JSON:API source adapter for ingestion (REQ-BRZ-04; plan section 3.2/3.3).

Real operator source documents (see ``data_samples/``) are TM Forum JSON:API
envelopes:
``{"data": {"type", "id", "attributes": {...}, "relationships": {...}}}`` with
quirks this module normalizes:

* relationship ids wrapped as ``"UUID(116100030984920447)"`` -> ``116100030984920447``
* money split into ``tax-free-amount`` + ``tax-amounts`` -> net / tax / gross
* nested ``attributes``/``event-details`` flattened

The adapter turns one envelope into one canonical record, ready to land in Bronze,
at the point where data enters the platform. Silver never sees envelopes; it
curates canonical records (REQ-SLV-02).
"""
from __future__ import annotations

import re
from typing import Any

_UUID_RE = re.compile(r"^UUID\((?P<inner>.*)\)$")


def strip_uuid_wrapper(value: Any) -> Any:
    """Turn ``"UUID(123)"`` into ``"123"``; pass through everything else."""
    if isinstance(value, str):
        m = _UUID_RE.match(value.strip())
        if m:
            return m.group("inner")
    return value


def normalize_money(total_charged_amount: dict | None) -> dict[str, Any]:
    """Flatten a TMF ``total-charged-amount`` block into net/tax/gross/currency."""
    if not total_charged_amount:
        return {"net_amount": 0.0, "tax_amount": 0.0, "gross_amount": 0.0, "currency": None}
    net_block = total_charged_amount.get("tax-free-amount", {}) or {}
    net = float(net_block.get("amount", 0.0) or 0.0)
    currency = net_block.get("currency")
    tax = 0.0
    for t in total_charged_amount.get("tax-amounts", []) or []:
        tax += float((t.get("tax-amount", {}) or {}).get("amount", 0.0) or 0.0)
    return {
        "net_amount": round(net, 5),
        "tax_amount": round(tax, 5),
        "gross_amount": round(net + tax, 5),
        "currency": currency,
    }


def _rel_id(relationships: dict, name: str) -> Any:
    """Resolve a relationship target id (handles single or list relationships)."""
    rel = (relationships or {}).get(name)
    if not rel:
        return None
    data = rel.get("data") if isinstance(rel, dict) else None
    if isinstance(rel, list):
        data = rel[0].get("data") if rel and isinstance(rel[0], dict) else None
    if isinstance(data, list):
        data = data[0] if data else None
    if isinstance(data, dict):
        return strip_uuid_wrapper(data.get("id"))
    return None


def unwrap_usage_event(doc: dict) -> dict[str, Any]:
    """Flatten a JSON:API voice/data usage event into the canonical schema."""
    attrs = doc["data"]["attributes"]
    details = attrs.get("event-details", {}) or {}
    money = normalize_money(attrs.get("total-charged-amount"))
    usu = (attrs.get("used-service-units") or [{}])[0]
    rels = doc["data"].get("relationships", {})
    return {
        "id": strip_uuid_wrapper(doc["data"]["id"]),
        "service_id": attrs.get("service-id"),
        "event_type": attrs.get("event-type"),
        "started_at": attrs.get("started-at"),
        "created_at": attrs.get("created-at"),
        "net_amount": money["net_amount"],
        "tax_amount": money["tax_amount"],
        "gross_amount": money["gross_amount"],
        "currency": money["currency"],
        "qty": usu.get("amount"),
        "unit": usu.get("unit-of-measure"),
        "is_roaming": details.get("is-roaming", False),
        "imei": details.get("imei"),
        "other_party_number": details.get("other-party-number"),
        "traffic_case": details.get("traffic-case"),
        "agreement_id": _rel_id(rels, "target"),
    }


def unwrap_recharge_event(doc: dict) -> dict[str, Any]:
    """Flatten a JSON:API recharge event into the canonical schema."""
    attrs = doc["data"]["attributes"]
    chars = attrs.get("characteristics", {}) or {}
    money = normalize_money({"tax-free-amount": (attrs.get("recharge-amount") or {}).get("tax-free-amount", {}),
                             "tax-amounts": (attrs.get("recharge-amount") or {}).get("tax-amounts", [])})
    rels = doc["data"].get("relationships", {})
    return {
        "id": strip_uuid_wrapper(doc["data"]["id"]),
        "service_id": attrs.get("service-id"),
        "created_at": attrs.get("created-at"),
        "amount": money["net_amount"],
        "currency": money["currency"],
        "payment_method": chars.get("payment-method"),
        "status": attrs.get("event-status"),
        "failure_reason_code": attrs.get("failure-reason-code"),
        "source": chars.get("source"),
        "recharge_type": attrs.get("recharge-type"),
        "agreement_id": _rel_id(rels, "target"),
    }
