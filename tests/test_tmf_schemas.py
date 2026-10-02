"""Vendored TMF schemas and the platform extension (REQ-BRZ-03/06)."""
from __future__ import annotations

import copy
import json
import re

import pytest

from idp.ingestion.tmf import schemas

RESOURCES = ["Individual", "Organization", "Customer", "BillingAccount", "Agreement", "Product",
             "AppliedCustomerBillingRate", "TopupBalance", "ProductUsage"]


def test_bundles_are_pinned_and_resolve_locally():
    # every bundle records its upstream source, every $ref resolves within the
    # vendored files (nothing is fetched), and every resource type compiles
    for name, bundle in schemas.bundles().items():
        src = bundle["x-source"]
        assert src["repo"].startswith("tmforum-apis/") and len(src["commit"]) == 40, name
        assert src["version"] in name
    schemas.merged_definitions()                     # raises on an unresolvable $ref
    assert set(schemas.resource_bundle()) >= set(RESOURCES)
    for resource in RESOURCES:
        schemas.validator(resource)
        schemas.validator(f"Idp{resource}")


def test_v4_resources_use_v5_party_references():
    agreement = schemas.bundles()["TMF651-v4.0.0.schema.json"]["$defs"]["Agreement"]
    assert agreement["properties"]["engagedParty"]["items"]["$ref"].endswith(
        "TMF632-v5.0.0.schema.json#/$defs/RelatedPartyRefOrPartyRoleRef")


def _usage(**extra):
    return {"id": "u1", "@type": "IdpProductUsage", "@baseType": "ProductUsage",
            "@schemaLocation": schemas.schema_location("IdpProductUsage"), "tenantId": "tenant_a",
            "usageType": "voice",
            "usageCharacteristic": [{"@type": "BooleanCharacteristic", "name": "isRoaming",
                                     "valueType": "boolean", "value": False}], **extra}


def test_extension_requires_tenant_id():
    assert schemas.first_error(_usage()) is None
    no_tenant = _usage(); del no_tenant["tenantId"]
    assert "tenantId" in schemas.first_error(no_tenant)


def test_typed_characteristics_are_enforced():
    bad = _usage(); bad["usageCharacteristic"][0]["value"] = "yes"   # BooleanCharacteristic
    assert "must be boolean" in schemas.first_error(bad)


@pytest.mark.parametrize("resource", RESOURCES)
def test_every_resource_has_a_platform_extension(resource):
    ext = schemas.extensions()[f"Idp{resource}"]
    assert ext["x-baseType"] == resource
    minimal = {"id": "x", "@type": f"Idp{resource}", "@baseType": resource, "tenantId": "t"}
    errs = list(schemas.errors(minimal))
    assert not [e for e in errs if "tenantId" in e]


def test_unknown_type_is_an_error():
    assert "no vendored TMF schema" in schemas.first_error({"id": "x", "@type": "Spaceship"})


# --------------------------------------------------------------------------- #
# create events: the Kafka envelope (REQ-STR-01/03)                            #
# --------------------------------------------------------------------------- #
# resources whose pinned spec defines no create event: the platform defines one
PLATFORM_EVENTS = {"ProductUsage", "AppliedCustomerBillingRate"}


def _envelope(resource, **extra):
    name = schemas.event_type(resource)
    return {"@type": name, "eventId": "e-1", "eventTime": "2026-01-01T00:00:00Z",
            "eventType": name, "event": {schemas.payload_name(resource): {"@type": "x"}}, **extra}


@pytest.mark.parametrize("resource", RESOURCES)
def test_every_resource_has_a_create_event(resource):
    # vendored from the resource's pinned spec, or defined by the platform on that
    # spec's own Event; either way it resolves locally and a sample validates
    name = schemas.event_type(resource)
    if resource in PLATFORM_EVENTS:
        assert name in schemas.events() and name not in schemas.resource_bundle()
        assert schemas.events()[name]["x-baseType"] == "Event"
    else:
        assert schemas.resource_bundle()[name] == schemas.resource_bundle()[resource]
    assert list(schemas.event_errors(_envelope(resource), resource)) == []


def test_envelope_is_checked():
    assert "eventType" in next(schemas.event_errors(_envelope("Product", eventType="ProductDeleteEvent"), "Product"))
    no_payload = _envelope("Product", event={})
    assert "product" in next(schemas.event_errors(no_payload, "Product"))
    no_id = _envelope("TopupBalance"); del no_id["eventId"]
    assert "eventId" in next(schemas.event_errors(no_id, "TopupBalance"))
    bad_time = _envelope("Agreement", eventTime="yesterday")           # v4 event, date-time
    assert next(schemas.event_errors(bad_time, "Agreement"), None) is not None
    assert next(schemas.event_errors([], "Agreement")).startswith("$: not an object")


@pytest.fixture(scope="module")
def tmf_resources():
    """One generated tenant as TMF resources, by @baseType."""
    from idp.common.records import to_records
    from idp.ingestion.tmf.profile import load_profile, to_tmf
    from idp.synthetic.generator import ENTITIES, generate_for_tenants

    ds = generate_for_tenants(["tenant_a"], subscribers=40, days=3, fraud_rate=0.2, seed=3)
    files = to_tmf(load_profile("standard"), {e: to_records(getattr(ds, e)) for e in ENTITIES})
    by_type: dict[str, list[dict]] = {}
    for resources in json.loads(json.dumps(files)).values():
        for r in resources:
            by_type.setdefault(r["@baseType"], []).append(r)
    return by_type


def test_registry_schemas_are_the_wire_contract(tmp_path, tmf_resources):
    # REQ-STR-03 / SCN-STR-03.1: one self-contained JSON Schema per topic, which
    # accepts its own create events (both party types on `party`) and no other topic's
    import fastjsonschema

    from idp.cli import main
    from idp.streaming import TOPIC_RESOURCES

    assert main(["registry-schemas", "--out", str(tmp_path)]) in (0, None)
    for suffix, resources in TOPIC_RESOURCES.items():
        path = tmp_path / f"{suffix}.schema.json"
        assert path.stat().st_size < 100_000, suffix
        check = fastjsonschema.compile(json.loads(path.read_text()))
        for topic, others in TOPIC_RESOURCES.items():
            for res in others:
                event = {**_envelope(res), "event": {schemas.payload_name(res): tmf_resources[res][0]}}
                if topic == suffix:
                    check(event)                                    # accepted
                else:
                    with pytest.raises(fastjsonschema.JsonSchemaException):
                        check(event)
    # the registered schema also checks the resource with its extension
    usage = copy.deepcopy(tmf_resources["ProductUsage"][0]); del usage["tenantId"]
    check = fastjsonschema.compile(json.loads((tmp_path / "productUsage.schema.json").read_text()))
    with pytest.raises(fastjsonschema.JsonSchemaException, match="tenantId"):
        check({**_envelope("ProductUsage"), "event": {"productUsage": usage}})
