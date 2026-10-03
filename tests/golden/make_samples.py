"""Regenerate the TM Forum reference samples in ``data_samples/`` (REQ-BRZ-03).

One resource per landing file type (usage: one voice, one data), produced by the
generator through the standard mapping profile, so they validate by construction.
They form one coherent chain: an organization, the customer that engages it, the
billing account it pays, the customer's agreement, and that agreement's product, fee
charge, recharge and usage; plus one individual.

  .venv/bin/python tests/golden/make_samples.py
"""
from __future__ import annotations

import json
from pathlib import Path

from idp.common.records import to_records
from idp.ingestion.tmf.profile import load_profile, to_tmf
from idp.synthetic.generator import ENTITIES, generate

OUT = Path(__file__).resolve().parents[2] / "data_samples"
PARAMS = {"subscribers": 12, "days": 3, "fraud_rate": 0.2, "seed": 42}


def main() -> None:
    ds = generate("tenant_a", **PARAMS)
    files = json.loads(json.dumps(to_tmf(load_profile("standard"),
                                         {e: to_records(getattr(ds, e)) for e in ENTITIES})))
    org = files["organization.json"][0]
    customer = next(c for c in files["customer.json"] if c["engagedParty"]["id"] == org["id"])
    account = next(a for a in files["billingAccount.json"] if a["id"] == customer["account"][0]["id"])
    agreement = next(a for a in files["agreement.json"]
                     if a["engagedParty"][0]["partyOrPartyRole"]["id"] == customer["id"])

    def of_agreement(resources, path):
        return [r for r in resources if path(r) == agreement["id"]]

    char = lambda r, name: next(c["value"] for c in r.get("usageCharacteristic", []) if c["name"] == name)
    product = of_agreement(files["product.json"], lambda r: r["agreementItem"][0]["agreementId"])[0]
    usage = of_agreement(files["productUsage.json"], lambda r: char(r, "agreementId"))
    samples = {
        "individual.json": files["individual.json"][0],
        "organization.json": org,
        "customer.json": customer,
        "billingAccount.json": account,
        "agreement.json": agreement,
        "product.json": product,
        "appliedCustomerBillingRate.json": next(f for f in files["appliedCustomerBillingRate.json"]
                                                if f["product"]["id"] == product["id"]),
        "topupBalance.json": of_agreement(files["topupBalance.json"], lambda r: r["agreement"]["id"])[0],
        "productUsage_voice.json": next(u for u in usage if u["usageType"] == "voice"),
        "productUsage_data.json": next(u for u in usage if u["usageType"] == "data"),
    }
    for name, resource in samples.items():
        (OUT / name).write_text(json.dumps(resource, indent=2) + "\n")
    print(f"Wrote {len(samples)} samples to {OUT}")


if __name__ == "__main__":
    main()
