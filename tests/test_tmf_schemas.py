"""Vendored TMF schemas and the platform extension (REQ-BRZ-03/06)."""
from __future__ import annotations

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
