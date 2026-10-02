"""Vendor the TM Forum Open API schemas the landing files use (REQ-BRZ-03/06).

Downloads each spec at a pinned upstream commit (tmforum-apis, Apache 2.0) and
writes one JSON Schema bundle per API to ``config/tmf/schemas/``. A bundle holds
only the definitions reachable from the resources the platform emits. The run
time never fetches anything: the bundles are committed.

The OpenAPI 3.0 schemas are converted to JSON Schema 2020-12, keeping their
meaning where the two differ:

* ``#/components/schemas/X`` (v5) and ``#/definitions/X`` (v4) become ``#/$defs/X``.
* An OpenAPI ``discriminator`` selects exactly one subtype by ``@type``. JSON Schema
  has no discriminator, and its ``oneOf`` would fail when the alternatives overlap
  (a ``PartyRoleRef`` also matches ``PartyRef``). So ``oneOf`` becomes ``anyOf``, and
  each discriminated type gets an explicit dispatch: ``if @type == T then $ref T``.
  The type's own content moves to ``X__core``, which the subtypes inherit from
  (``allOf``), so validating a subtype never re-enters the dispatch.
* ``example``/``examples`` and the ``discriminator`` itself are dropped.

TMF651 (Agreement) and TMF654 (Prepay Balance) have no stable public v5 schema. They
are vendored from v4.0.0, and their party references and characteristics are
rewritten to the v5 types (``RelatedPartyRefOrPartyRoleRef`` and the polymorphic
``Characteristic`` of TMF632 v5), so every landing resource follows v5 conventions.

  .venv/bin/python config/tmf/vendor_schemas.py      # rewrites config/tmf/schemas/
"""
from __future__ import annotations

import json
from pathlib import Path

import requests
import yaml

OUT = Path(__file__).resolve().parent / "schemas"
RAW = "https://raw.githubusercontent.com/tmforum-apis/{repo}/{sha}/{file}"
BASE_ID = "https://idp.local/tmf/schemas/"

# api -> (repo, file, commit, version, root resources)
SOURCES = {
    "TMF629": ("TMF629_CustomerManagement", "TMF629-Customer_Management-v5.0.1.oas.yaml",
               "1eae32ff68230c0cb7969d69fb9711d1fff77b39", "5.0.1", ["Customer"]),
    "TMF632": ("TMF632_PartyManagement", "TMF632-Party_Management-v5.0.0.oas.yaml",
               "11c6a0ecf2b9a7007f43e86601b37e45d9f39b1e", "5.0.0",
               ["Individual", "Organization", "RelatedPartyRefOrPartyRoleRef", "Characteristic"]),
    "TMF635": ("TMF635_UsageManagement", "TMF635-Product_Usage_Management-v5.0.0.oas.yaml",
               "3be6eb8098ff4026044a984d49ed624d1ce82825", "5.0.0", ["ProductUsage"]),
    "TMF637": ("TMF637_ProductInventory", "TMF637-ProductInventory-v5.0.0.oas.yaml",
               "29d0ef17db906c26a6b34e06c78263ef3e0dfb1f", "5.0.0", ["Product"]),
    "TMF666": ("TMF666_AccountManagement", "TMF666-Account_Management-v5.0.0.oas.yaml",
               "8c85948d31c73ad7408595dd485cf7f00573f59f", "5.0.0", ["BillingAccount"]),
    "TMF678": ("TMF678_CustomerBill", "TMF678-CustomerBill-v5.0.0.oas.yaml",
               "eb90402dc362a602225476245398887ce484f9cb", "5.0.0", ["AppliedCustomerBillingRate"]),
    "TMF651": ("TMF651_AgreementManagement", "TMF651_Agreement_Management_API_v4.0.0_swagger.json",
               "756dee9649778130a31a7551ac21a20c7a6106af", "4.0.0", ["Agreement"]),
    "TMF654": ("TMF654_PrepayBalanceManagement", "TMF654_Prepay_Balance_Management_API_v4.0.0_swagger.json",
               "a19a388d1ac02afe687c4ba721157a81c3f3e463", "4.0.0", ["TopupBalance"]),
}
# v4 resources rewritten onto v5 types: (resource, property) -> v5 TMF632 definition
V5_PATCHES = {
    ("Agreement", "engagedParty"): "RelatedPartyRefOrPartyRoleRef",
    ("Agreement", "characteristic"): "Characteristic",
    ("TopupBalance", "relatedParty"): "RelatedPartyRefOrPartyRoleRef",
}
DROP = {"example", "examples", "discriminator"}


def bundle_file(api: str) -> str:
    return f"{api}-v{SOURCES[api][3]}.schema.json"


def _fetch(api: str) -> dict:
    repo, file, sha, _, _ = SOURCES[api]
    resp = requests.get(RAW.format(repo=repo, sha=sha, file=file), timeout=60)
    resp.raise_for_status()
    text = resp.text
    doc = yaml.safe_load(text)                      # YAML is a superset of JSON
    return doc["components"]["schemas"] if "components" in doc else doc["definitions"]


def _refs(node, prefix):
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith(prefix):
            yield ref[len(prefix):]
        for v in node.values():
            yield from _refs(v, prefix)
    elif isinstance(node, list):
        for v in node:
            yield from _refs(v, prefix)


def _closure(defs: dict, roots: list[str], prefix: str) -> list[str]:
    seen, todo = [], list(roots)
    while todo:
        name = todo.pop()
        if name in seen or name not in defs:
            continue
        seen.append(name)
        todo.extend(_refs(defs[name], prefix))
        for target in ((defs[name].get("discriminator") or {}).get("mapping") or {}).values():
            if isinstance(target, str):
                todo.append(target.split("/")[-1])
    return sorted(seen)


def _convert(node, prefix):
    """OpenAPI -> JSON Schema: rewrite refs, oneOf -> anyOf, drop OAS-only keywords."""
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k in DROP:
                continue
            if k == "$ref" and isinstance(v, str) and v.startswith(prefix):
                out[k] = "#/$defs/" + v[len(prefix):]
            elif k == "oneOf":
                out["anyOf"] = _convert(v, prefix)
            else:
                out[k] = _convert(v, prefix)
        return out
    if isinstance(node, list):
        return [_convert(v, prefix) for v in node]
    return node


def _dispatch(defs_in: dict, names: list[str], prefix: str) -> dict:
    """Converted $defs with an explicit @type dispatch for every discriminated type."""
    out = {n: _convert(defs_in[n], prefix) for n in names}
    discriminated = {}
    for n in names:
        mapping = ((defs_in[n].get("discriminator") or {}).get("mapping") or {})
        subs = {t: ref.split("/")[-1] for t, ref in mapping.items()
                if isinstance(ref, str) and ref.split("/")[-1] != n and ref.split("/")[-1] in out}
        if subs:
            discriminated[n] = subs
    # subtypes inherit from the base's core, never from its dispatching form
    for n, schema in out.items():
        for part in schema.get("allOf", []):
            target = part.get("$ref", "")[len("#/$defs/"):]
            if target in discriminated and n in discriminated[target].values():
                part["$ref"] = f"#/$defs/{target}__core"
    for base, subs in discriminated.items():
        out[f"{base}__core"] = out[base]
        out[base] = {"allOf": [{"$ref": f"#/$defs/{base}__core"}] + [
            {"if": {"properties": {"@type": {"const": t}}, "required": ["@type"]},
             "then": {"$ref": f"#/$defs/{sub}"}} for t, sub in sorted(subs.items())]}
    return out


def build(api: str, fetched: dict) -> dict:
    repo, file, sha, version, roots = SOURCES[api]
    defs = fetched
    prefix = "#/components/schemas/" if version.startswith("5") else "#/definitions/"
    names = _closure(defs, roots, prefix)
    out = _dispatch(defs, names, prefix)
    for (resource, prop), v5_def in V5_PATCHES.items():
        if resource in out:
            target = {"$ref": f"{bundle_file('TMF632')}#/$defs/{v5_def}"}
            for part in [out[resource]] + out[resource].get("allOf", []):
                if prop in part.get("properties", {}):
                    part["properties"][prop] = {"type": "array", "items": target}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": BASE_ID + bundle_file(api),
        "title": f"{api} v{version} resources used by the IDP landing zone",
        "x-source": {"repo": f"tmforum-apis/{repo}", "file": file, "commit": sha,
                     "version": version, "license": "Apache-2.0",
                     "v5Conventions": "native" if version.startswith("5") else "rewritten (see vendor_schemas.py)"},
        "x-resources": roots,
        "$defs": out,
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for api in SOURCES:
        bundle = build(api, _fetch(api))
        (OUT / bundle_file(api)).write_text(json.dumps(bundle, indent=1, sort_keys=True) + "\n")
        print(f"{bundle_file(api)}: {len(bundle['$defs'])} definitions")


if __name__ == "__main__":
    main()
