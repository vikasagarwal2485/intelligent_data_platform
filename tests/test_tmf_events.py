"""TMF create events: the Kafka envelope (REQ-STR-01/02)."""
from __future__ import annotations

import copy
import json

import pytest

from idp.common.records import to_records
from idp.ingestion.tmf.events import EventError, event_id, from_event, to_event
from idp.ingestion.tmf.profile import load_profile, to_tmf
from idp.synthetic.generator import ENTITIES, generate_for_tenants


@pytest.fixture(scope="module")
def files():
    ds = generate_for_tenants(["tenant_a"], subscribers=30, days=2, fraud_rate=0.2, seed=4)
    return json.loads(json.dumps(to_tmf(load_profile("standard"),
                                        {e: to_records(getattr(ds, e)) for e in ENTITIES})))


def test_every_resource_round_trips_through_its_event(files):
    for name, resources in files.items():
        for res in resources[:3]:
            base = res["@baseType"]
            event = to_event(res, "tenant_a")
            assert event["eventType"] == event["@type"] == f"{base}CreateEvent"
            assert event["eventTime"].endswith("Z")
            assert from_event(json.loads(json.dumps(event)), [base], "tenant_a") == res, name


def test_event_ids_are_stable(files):
    # SCN-STR-01.8: the same resource gets the same eventId; another resource or
    # tenant gets another
    a, b = files["agreement.json"][:2]
    assert to_event(a, "tenant_a")["eventId"] == to_event(copy.deepcopy(a), "tenant_a")["eventId"]
    assert event_id("tenant_a", a) != event_id("tenant_a", b)
    assert event_id("tenant_a", a) != event_id("tenant_b", a)


def test_party_topic_accepts_both_party_events(files):
    party = ["Individual", "Organization"]
    for name in ("individual.json", "organization.json"):
        res = files[name][0]
        assert from_event(to_event(res, "tenant_a"), party, "tenant_a") == res


def test_bad_events_are_rejected_with_a_reason(files):
    usage = files["productUsage.json"][0]
    good = to_event(usage, "tenant_a")
    cases = {
        "isn't a create event of this topic": (good, ["TopupBalance"], "tenant_a"),
        "invalid event": ({**good, "eventId": ""}, ["ProductUsage"], "tenant_a"),
        "not the topic's tenant 'tenant_b'": (good, ["ProductUsage"], "tenant_b"),
    }
    broken = copy.deepcopy(good)
    broken["event"]["productUsage"]["usageProductPrice"][0]["price"]["dutyFreeAmount"]["value"] = "1.0"
    cases["invalid ProductUsage"] = (broken, ["ProductUsage"], "tenant_a")
    wrong = copy.deepcopy(good)
    wrong["event"]["productUsage"]["@baseType"] = "Product"
    cases["carries a 'Product'"] = (wrong, ["ProductUsage"], "tenant_a")
    for reason, (value, resources, tenant) in cases.items():
        with pytest.raises(EventError, match=reason.replace("(", r"\(")):
            from_event(value, resources, tenant)
    with pytest.raises(EventError, match="isn't a create event"):
        from_event(["not", "an", "object"], ["ProductUsage"], "tenant_a")
