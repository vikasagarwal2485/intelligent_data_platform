"""TM Forum mapping profiles: the engine and the standard profile (REQ-BRZ-05/06)."""
from __future__ import annotations

import copy
import json

import pytest

from idp.common.records import to_records
from idp.ingestion.tmf import schemas
from idp.ingestion.tmf.profile import (ProfileError, ReferenceError_, from_tmf, get_path,
                                       load_profile, parse_profile, set_path, to_tmf)
from idp.synthetic.generator import ENTITIES, generate_for_tenants

TENANTS = ("tenant_a", "tenant_b")


@pytest.fixture(scope="module")
def dataset():
    return generate_for_tenants(list(TENANTS), subscribers=150, days=10, fraud_rate=0.2, seed=11)


@pytest.fixture(scope="module")
def profile():
    return load_profile("standard")


def _canonical(ds, tenant):
    return {e: to_records(getattr(ds, e)[getattr(ds, e)["tenant_id"] == tenant]) for e in ENTITIES}


def _json(x):
    return json.loads(json.dumps(x))


# --------------------------------------------------------------------------- #
# engine (task 1.3)                                                            #
# --------------------------------------------------------------------------- #
def test_paths():
    obj: dict = {}
    set_path(obj, "a[1].b.c", 5)
    assert obj == {"a": [{}, {"b": {"c": 5}}]} and get_path(obj, "a[1].b.c") == 5
    assert get_path(obj, "a[3].b") is None and get_path(obj, "x.y") is None


TOY = {
    "name": "toy",
    "customer": {"resource": "Customer", "file": "customer.json"},
    "resources": [{
        "entity": "things", "resource": "Product", "type": "Product", "file": "thing.json",
        "set": {"status": "active"},
        "fields": {
            "id": {"path": "id"},
            "kind": {"char": "kind", "in": "productCharacteristic", "type": "string"},
            "state": {"path": "status", "map": {"on": "active", "off": "suspended"}},
            "net": {"path": "productPrice[0].price.dutyFreeAmount.value"},
            "tax": {"sum": "productPrice[0].price.taxIncludedAmount.value", "of": "net", "round": 6},
            "note": "absent",
        },
    }],
}


def test_toy_profile_round_trip():
    prof = parse_profile(copy.deepcopy(TOY))
    recs = [{"id": "1", "kind": "x", "state": "off", "net": 0.18, "tax": 0.018, "note": None},
            {"id": "2", "kind": None, "state": "on", "net": None, "tax": None, "note": None}]
    files = to_tmf(prof, {"things": recs})
    [first, second] = files["thing.json"]
    assert first["status"] == "suspended" and first["productCharacteristic"] == [
        {"@type": "StringCharacteristic", "name": "kind", "valueType": "string", "value": "x"}]
    assert "productCharacteristic" not in second                    # null -> omitted
    assert from_tmf(prof, _json(files))["things"] == recs           # tax recovered exactly


@pytest.mark.parametrize("broken, message", [
    ({"twice": {"path": "a", "char": "b", "in": "c", "type": "string"}}, "exactly one"),
    ({"bad_char": {"char": "b", "in": "c", "type": "date"}}, "characteristic type"),
    ({"bad_map": {"path": "a", "map": {"x": "y", "z": "y"}}}, "one-to-one"),
    ({"bad_sum": {"sum": "a", "of": "later"}, "later": {"path": "b"}}, "mapped before"),
    ({"bad_via": {"path": "a", "via": "nowhere"}}, "unknown relation"),
])
def test_profile_errors_are_caught_on_load(broken, message):
    doc = copy.deepcopy(TOY)
    doc["resources"][0]["fields"].update(broken)
    with pytest.raises(ProfileError, match=message):
        parse_profile(doc)


def test_variants_must_map_the_same_fields():
    doc = copy.deepcopy(TOY)
    other = copy.deepcopy(doc["resources"][0])
    other["fields"].pop("note")
    doc["resources"][0]["when"] = {"state": "on"}
    other["when"] = {"state": "off"}
    doc["resources"].append(other)
    with pytest.raises(ProfileError, match="same fields"):
        parse_profile(doc)


# --------------------------------------------------------------------------- #
# the standard profile (task 3.1)                                              #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("tenant", TENANTS)
def test_standard_profile_round_trip(dataset, profile, tenant):
    # SCN-BRZ-05.1 — byte-for-byte: the same JSON records the in-memory producer sends
    canonical = _canonical(dataset, tenant)
    back = from_tmf(profile, _json(to_tmf(profile, canonical)))
    for entity in ENTITIES:
        assert json.dumps(back[entity]) == json.dumps(canonical[entity]), entity


def test_profile_columns_are_the_canonical_columns(dataset, profile):
    for entity in ENTITIES:
        assert profile.entity_columns(entity) == list(getattr(dataset, entity).columns), entity


def _allowed_props(resource_type: str) -> set[str]:
    """Top-level properties a resource may carry: its TMF type's (through allOf)
    plus its extension's."""
    ext = schemas.extensions()[resource_type]
    base = ext["x-baseType"]
    bundle = schemas.bundles()[schemas.resource_bundle()[base]]
    defs, seen, props = bundle["$defs"], set(), set()

    def walk(name):
        if name in seen or name not in defs:
            return
        seen.add(name)
        d = defs[name]
        props.update(d.get("properties", {}))
        for part in d.get("allOf", []):
            props.update(part.get("properties", {}))
            ref = part.get("$ref", "")
            if ref.startswith("#/$defs/"):
                walk(ref.split("/")[-1])
    walk(base)
    walk(f"{base}__core")
    for part in ext["allOf"]:
        props.update(part.get("properties", {}))
    return props


@pytest.mark.parametrize("tenant", TENANTS)
def test_standard_profile_emits_valid_plain_tmf(dataset, profile, tenant):
    # SCN-BRZ-03.5 / SCN-BRZ-06.1 — every resource validates; every top-level field is a
    # standard field of its type or a platform-extension attribute; tenantId everywhere
    files = to_tmf(profile, _canonical(dataset, tenant))
    assert set(files) == {"individual.json", "organization.json", "customer.json",
                          "billingAccount.json", "agreement.json", "product.json",
                          "appliedCustomerBillingRate.json", "topupBalance.json", "productUsage.json"}
    for name, resources in files.items():
        allowed = _allowed_props(resources[0]["@type"])
        for r in resources:
            assert schemas.first_error(r) is None, (name, r["id"], schemas.first_error(r))
            assert r["tenantId"] == tenant
            assert set(r) <= allowed, (name, set(r) - allowed)


# --------------------------------------------------------------------------- #
# adapter behaviour (task 3.2)                                                 #
# --------------------------------------------------------------------------- #
def test_organization_reached_through_the_customer_role(dataset, profile):
    # SCN-BRZ-05.2
    canonical = _canonical(dataset, "tenant_a")
    files = _json(to_tmf(profile, canonical))
    org = files["organization.json"][0]
    customer = next(c for c in files["customer.json"] if c["engagedParty"]["id"] == org["id"])
    assert customer["engagedParty"]["@referredType"] == "Organization"
    account_ref = customer["account"][0]["id"]
    acct = next(a for a in files["billingAccount.json"] if a["id"] == account_ref)
    assert acct["relatedParty"][0]["partyOrPartyRole"] == {
        "@type": "PartyRoleRef", "id": customer["id"], "@referredType": "Customer"}
    back = from_tmf(profile, files)
    row = next(a for a in back["accounts"] if a["id"] == account_ref)
    assert row["party_id"] == org["id"] and row["is_company"] is True
    party = next(p for p in back["parties"] if p["id"] == org["id"])
    assert party["party_type"] == "organization"
    assert party["first_name"] is party["last_name"] is party["date_of_birth"] is None


def test_usage_split_by_type_with_amounts(dataset, profile):
    # SCN-BRZ-05.3
    canonical = _canonical(dataset, "tenant_a")
    files = _json(to_tmf(profile, canonical))
    kinds = {u["usageType"] for u in files["productUsage.json"]}
    assert kinds == {"voice", "data"}
    back = from_tmf(profile, files)
    assert {r["event_type"] for r in back["voice_events"]} == {"voice"}
    assert {r["event_type"] for r in back["data_events"]} == {"data"}
    v = canonical["voice_events"][0]
    usage = next(u for u in files["productUsage.json"] if u["id"] == v["id"])
    price = usage["usageProductPrice"][0]["price"]
    assert price["dutyFreeAmount"] == {"unit": v["currency"], "value": v["net_amount"]}
    assert price["taxIncludedAmount"]["value"] == pytest.approx(v["net_amount"] + v["tax_amount"])


def test_dangling_or_ambiguous_references_name_the_file(dataset, profile):
    # SCN-BRZ-05.4
    files = _json(to_tmf(profile, _canonical(dataset, "tenant_a")))
    dangling = copy.deepcopy(files)
    dangling["agreement.json"][0]["engagedParty"][0]["partyOrPartyRole"]["id"] = "tenant_a-cust-nope"
    with pytest.raises(ReferenceError_) as e:
        from_tmf(profile, dangling)
    assert e.value.file == "agreement.json" and "tenant_a-cust-nope" in str(e.value)

    ambiguous = copy.deepcopy(files)
    cust_id = ambiguous["agreement.json"][0]["engagedParty"][0]["partyOrPartyRole"]["id"]
    cust = next(c for c in ambiguous["customer.json"] if c["id"] == cust_id)
    cust["account"].append({"@type": "AccountRef", "id": "tenant_a-acct-other"})
    with pytest.raises(ReferenceError_, match="ambiguous"):
        from_tmf(profile, ambiguous)


def test_absent_field_with_a_value_is_refused(profile):
    # an Organization can't carry a first name: writing one would silently lose it
    rec = {"tenant_id": "t", "id": "t-party-1", "role": "owner", "first_name": "Ann", "last_name": None,
           "date_of_birth": None, "language": "en", "phone": None, "email": None,
           "consent_marketing": True, "consent_analytics": True, "account_id": "t-acct-1",
           "party_type": "organization", "organization_name": "Co", "organization_type": "company"}
    with pytest.raises(ValueError, match="doesn't carry first_name"):
        to_tmf(profile, {"parties": [rec]})


def test_customer_extension_profile(dataset, monkeypatch, tmp_path):
    # SCN-BRZ-06.2 — a customer @type subtype, with its own schema on top of the
    # platform extension, carries a canonical field as an extension attribute
    import shutil

    import yaml

    from idp.ingestion.tmf import profile as profile_mod

    ext_dir = tmp_path / "extensions"
    shutil.copytree(schemas.EXTENSION_DIR, ext_dir)
    (ext_dir / "AcmeProductUsage.schema.json").write_text(json.dumps({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://idp.local/tmf/extensions/AcmeProductUsage.schema.json",
        "x-baseType": "ProductUsage",
        "allOf": [{"$ref": "IdpProductUsage.schema.json"},
                  {"type": "object", "required": ["networkGeneration"],
                   "properties": {"networkGeneration": {"type": "string", "enum": ["2G", "3G", "4G", "4G+", "5G"]}}}],
    }))
    monkeypatch.setattr(schemas, "EXTENSION_DIR", ext_dir)
    for cached in (schemas.extensions, schemas.merged_definitions, schemas.validator):
        cached.cache_clear()

    doc = yaml.safe_load((profile_mod.PROFILE_DIR / "standard.yaml").read_text())
    data = next(r for r in doc["resources"] if r["entity"] == "data_events")
    data = json.loads(json.dumps(data))                       # unshare the YAML anchors
    data["type"] = "AcmeProductUsage"
    data["fields"]["network_tech"] = {"ext": "networkGeneration"}
    doc["resources"] = [data if r["entity"] == "data_events" else r for r in doc["resources"]]
    acme = parse_profile(doc, "acme")
    try:
        canonical = _canonical(dataset, "tenant_a")
        files = _json(to_tmf(acme, canonical))
        data_usage = [u for u in files["productUsage.json"] if u["usageType"] == "data"]
        u = data_usage[0]
        assert u["@type"] == "AcmeProductUsage" and u["@baseType"] == "ProductUsage"
        assert u["@schemaLocation"].endswith("/AcmeProductUsage.schema.json")
        assert "networkGeneration" in u and not any(
            c["name"] == "networkTechnology" for c in u["usageCharacteristic"])
        assert all(schemas.first_error(x) is None for x in data_usage)
        bad = dict(u, networkGeneration="6G")
        assert "must be one of" in schemas.first_error(bad)        # the customer schema applies
        back = from_tmf(acme, files)
        assert json.dumps(back["data_events"]) == json.dumps(canonical["data_events"])
    finally:
        for cached in (schemas.extensions, schemas.merged_definitions, schemas.validator):
            cached.cache_clear()
