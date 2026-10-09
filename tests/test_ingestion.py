"""Tests for the file landing zone (REQ-BRZ-03) and the shared entity list.

Bronze itself (Iceberg, `pipeline.to_bronze`) is covered by test_bronze_iceberg.py.
"""
from __future__ import annotations

import pytest

from idp.ingestion import landing
from idp.synthetic.generator import ENTITIES, generate_for_tenants


TMF_FILES = {"individual.json", "organization.json", "customer.json", "billingAccount.json",
             "agreement.json", "product.json", "appliedCustomerBillingRate.json", "topupBalance.json",
             "productUsage.json"}


@pytest.fixture(scope="module")
def landing_dataset():
    """A small two-tenant dataset that still yields every TMF landing file for both
    tenants (organizations and a month of fee charges included). Writing and
    validating the files is pure Python, so the size sets these tests' run time."""
    return generate_for_tenants(["tenant_a", "tenant_b"], subscribers=15, days=31,
                                fraud_rate=0.2, seed=7)


def test_write_landing_partitions_by_tenant(landing_dataset, tmp_path):
    written = landing.write_landing(landing_dataset, tmp_path)
    assert set(written) <= TMF_FILES and "productUsage.json" in written
    assert (tmp_path / "tenant=tenant_a" / "individual.json").exists()
    assert (tmp_path / "tenant=tenant_b" / "customer.json").exists()


def test_landing_files_are_tmf_resources_of_their_tenant(landing_dataset, tmp_path):
    # SCN-BRZ-03.1 / 03.5 — TMF resource files, each resource valid and tenant-stamped
    import json

    from idp.ingestion.tmf import schemas

    landing.write_landing(landing_dataset, tmp_path)
    for tenant_id in ("tenant_a", "tenant_b"):
        folder = tmp_path / f"tenant={tenant_id}"
        names = {p.name for p in folder.iterdir()}
        assert names <= TMF_FILES and {"customer.json", "productUsage.json", "agreement.json"} <= names
        for path in folder.iterdir():
            resources = json.loads(path.read_text(encoding="utf-8"))
            assert resources and {r["tenantId"] for r in resources} == {tenant_id}
            assert all(r["@baseType"] and r["href"].endswith(r["id"]) for r in resources)
            assert all(schemas.first_error(r) is None for r in resources), path.name


def test_generate_command_writes_landing_not_bronze(tmp_path):
    import yaml

    from idp.cli import main
    from idp.common.config import DEFAULT_CONFIG_PATH

    raw = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["storage"] = {k: str(tmp_path / k) for k in
                      ("root", "landing", "bronze", "silver", "gold", "ml_artifacts")}
    raw["synthetic"].update({"subscribers_per_tenant": 10, "days": 5})
    cfg_path = tmp_path / "platform.yaml"
    cfg_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    assert main(["--config", str(cfg_path), "generate"]) == 0
    assert (tmp_path / "landing" / "tenant=tenant_a" / "customer.json").exists()
    assert not (tmp_path / "bronze").exists()


# --- one entity list ----------------------------------------------------------
def test_entities_defined_once_and_cover_dataset_and_topics():
    from dataclasses import fields

    from idp import pipeline
    from idp.streaming import BRONZE_ENTITIES
    from idp.synthetic import generator
    from idp.synthetic.generator import ENTITIES, SyntheticDataset

    assert pipeline.ENTITIES is generator.ENTITIES                 # a re-export, not a copy
    assert ENTITIES == tuple(f.name for f in fields(SyntheticDataset) if f.name != "labels")
    assert BRONZE_ENTITIES == ENTITIES                 # the consumer lands exactly the dataset's entities


# --- landing location (REQ-BRZ-03) --------------------------------------------
def test_landing_uri_local_and_s3(monkeypatch):
    from idp.common import storage
    from idp.common.config import load_config
    monkeypatch.delenv("IDP_S3_ENDPOINT", raising=False)
    cfg = load_config()
    assert storage.landing_uri(cfg) == cfg.storage.landing
    monkeypatch.setenv("IDP_S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("IDP_S3_BUCKET_PREFIX", "idp")
    assert storage.landing_uri(load_config()) == "s3://idp-landing"   # its own bucket, not a lake layer


def test_landing_on_object_storage_matches_local(landing_dataset, tmp_path):
    # SCN-BRZ-03.3 — an fsspec in-memory bucket stands in for s3://idp-landing
    import fsspec
    landing.write_landing(landing_dataset, tmp_path)
    landing.write_landing(landing_dataset, "memory://idp-landing")
    mem = fsspec.filesystem("memory")
    local = {p.relative_to(tmp_path).as_posix(): p.read_bytes() for p in tmp_path.rglob("*.json")}
    remote = {p.split("idp-landing/", 1)[1]: mem.cat_file(p) for p in mem.find("/idp-landing")}
    assert remote == local and local
    mem.rm("/idp-landing", recursive=True)


# --- reading the landing zone for publishing (REQ-STR-01) ----------------------
def _read(root, tenants=("tenant_a", "tenant_b")):
    return list(landing.read_landing(root, tenants))


def test_read_landing_round_trip_in_publish_order(landing_dataset, tmp_path):
    # SCN-BRZ-05.1 through files: the canonical records, entity by entity, in publish order
    import json

    from idp.common.records import to_records

    landing.write_landing(landing_dataset, tmp_path)
    files = _read(tmp_path)
    assert all(f.reason is None for f in files)
    assert [f.tenant for f in files] == sorted(f.tenant for f in files)          # tenant by tenant
    for tenant in ("tenant_a", "tenant_b"):
        mine = [f for f in files if f.tenant == tenant]
        assert [f.entity for f in mine] == [e for e in ENTITIES if any(m.entity == e for m in mine)]
        for f in mine:
            df = getattr(landing_dataset, f.entity)
            assert json.dumps(f.records) == json.dumps(to_records(df[df["tenant_id"] == tenant]))


def _land(dataset, root):
    import json
    landing.write_landing(dataset, root)
    folder = root / "tenant=tenant_a"
    return folder, (lambda name: json.loads((folder / name).read_text()))


def _write(path, payload):
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload if isinstance(payload, str) else json.dumps(payload))


def test_read_landing_rejects_bad_files_and_keeps_good_ones(landing_dataset, tmp_path):
    folder, load = _land(landing_dataset, tmp_path)
    topups = load("topupBalance.json")
    topups[0]["tenantId"] = "tenant_b"                                     # SCN-STR-01.4
    _write(folder / "topupBalance.json", topups)
    fees = load("appliedCustomerBillingRate.json")
    fees[0]["taxIncludedAmount"]["value"] = "lots"                         # SCN-BRZ-06.3
    _write(folder / "appliedCustomerBillingRate.json", fees)
    agreements = load("agreement.json")
    agreements[0]["engagedParty"][0]["partyOrPartyRole"]["id"] = "tenant_a-cust-gone"   # SCN-BRZ-05.4
    _write(folder / "agreement.json", agreements)
    _write(folder / "notes.json", [])                                      # unknown file
    _write(folder / "billingAccount.json", "{not json")                    # invalid json
    _write(folder / "product.json", {"id": "x"})                           # not an array
    _write(tmp_path / "tenant=tenant_x" / "individual.json", [])           # SCN-STR-01.5
    _write(tmp_path / "stray.json", [])                                    # not a tenant folder
    by_name = {(f.tenant, f.entity or f.path.rsplit("/", 1)[-1]): f for f in _read(tmp_path)}

    reasons = {k: f.reason for k, f in by_name.items() if f.reason}
    assert reasons[("tenant_a", "topupBalance")].startswith("tenant mismatch: 1")
    assert reasons[("tenant_a", "appliedCustomerBillingRate")].startswith("invalid resource")
    assert reasons[("tenant_a", "notes")].startswith("unknown landing file")
    assert reasons[("tenant_a", "billingAccount")].startswith("invalid json")
    assert reasons[("tenant_a", "product")].startswith("invalid json")
    assert reasons[("tenant_x", "individual")].startswith("unknown tenant")
    assert reasons[("", "stray.json")] == "not a tenant folder"
    # the entities adapted from rejected files, or holding a dangling reference
    for entity, file in (("recharge_events", "topupBalance.json"), ("fee_charges", "appliedCustomerBillingRate.json"),
                         ("billing_accounts", "billingAccount.json"), ("products", "product.json")):
        assert reasons[("tenant_a", entity)] == f"depends on rejected file {file!r}"
        assert by_name[("tenant_a", entity)].records is None                 # never partly returned
    assert "tenant_a-cust-gone" in reasons[("tenant_a", "agreements")]
    # everything else still goes, including the other tenant
    for entity in ("parties", "customers", "voice_events", "data_events"):
        assert by_name[("tenant_a", entity)].records
    assert all(f.reason is None for k, f in by_name.items() if k[0] == "tenant_b")


def test_read_landing_missing_zone_yields_nothing(tmp_path):
    assert _read(tmp_path / "absent") == []
