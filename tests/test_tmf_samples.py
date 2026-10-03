"""The TM Forum v5 reference samples in data_samples/ (REQ-BRZ-03/05)."""
from __future__ import annotations

import json
from pathlib import Path

from idp.ingestion.tmf import schemas
from idp.ingestion.tmf.profile import load_profile

SAMPLES = Path(__file__).resolve().parents[1] / "data_samples"


def _sample(name):
    return json.loads((SAMPLES / name).read_text())


def test_every_sample_is_a_valid_tmf_resource():
    for path in sorted(SAMPLES.glob("*.json")):
        resource = json.loads(path.read_text())
        assert schemas.first_error(resource) is None, path.name
        assert resource["tenantId"] and resource["@baseType"]


def test_samples_cover_every_landing_file():
    stems = {p.stem.split("_")[0] for p in SAMPLES.glob("*.json")}
    assert stems == {f.removesuffix(".json") for f in load_profile().files()}
    assert {_sample(f"productUsage_{k}.json")["usageType"] for k in ("voice", "data")} == {"voice", "data"}


def test_samples_form_one_chain():
    # the organization pays the billing account, the customer engages it and holds
    # that account, and the agreement belongs to the customer
    org, customer, account = _sample("organization.json"), _sample("customer.json"), _sample("billingAccount.json")
    agreement = _sample("agreement.json")
    assert customer["engagedParty"] == {"id": org["id"], "@type": "PartyRef"}
    assert account["relatedParty"][0]["role"] == "payer"
    assert account["relatedParty"][0]["partyOrPartyRole"] == {
        "@type": "PartyRef", "id": org["id"], "@referredType": "Organization"}
    assert customer["account"][0]["id"] == account["id"]
    assert agreement["engagedParty"][0]["role"] == "customer"
    assert agreement["engagedParty"][0]["partyOrPartyRole"]["id"] == customer["id"]
    assert {c["name"] for c in customer["characteristic"]} == {"segment"}
