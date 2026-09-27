"""Tests for the file landing zone (REQ-BRZ-03) and the shared entity list.

Bronze itself (Iceberg, `pipeline.to_bronze`) is covered by test_bronze_iceberg.py.
"""
from __future__ import annotations

from idp.ingestion import landing
from idp.synthetic.generator import ENTITIES


def test_write_landing_partitions_by_tenant(multi_tenant_dataset, tmp_path):
    written = landing.write_landing(multi_tenant_dataset, tmp_path)
    assert "parties" in written
    assert (tmp_path / "tenant=tenant_a" / "parties.json").exists()
    assert (tmp_path / "tenant=tenant_b" / "parties.json").exists()


def test_landing_has_one_file_per_tenant_entity(multi_tenant_dataset, tmp_path):
    import json

    landing.write_landing(multi_tenant_dataset, tmp_path)
    for tenant_id in ("tenant_a", "tenant_b"):
        for entity in ENTITIES:
            if getattr(multi_tenant_dataset, entity).empty:
                continue
            path = tmp_path / f"tenant={tenant_id}" / f"{entity}.json"
            records = json.loads(path.read_text(encoding="utf-8"))
            assert records and {r["tenant_id"] for r in records} == {tenant_id}


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
    assert (tmp_path / "landing" / "tenant=tenant_a" / "parties.json").exists()
    assert not (tmp_path / "bronze").exists()


# --- one entity list ----------------------------------------------------------
def test_entities_defined_once_and_cover_dataset_and_topics():
    from dataclasses import fields

    from idp import pipeline
    from idp.streaming import TABLE_TO_SUFFIX
    from idp.synthetic import generator
    from idp.synthetic.generator import ENTITIES, SyntheticDataset

    assert pipeline.ENTITIES is generator.ENTITIES                 # a re-export, not a copy
    assert ENTITIES == tuple(f.name for f in fields(SyntheticDataset) if f.name != "labels")
    assert tuple(TABLE_TO_SUFFIX) == ENTITIES          # same set AND order (landing = in-memory publish order)


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


def test_landing_on_object_storage_matches_local(multi_tenant_dataset, tmp_path):
    # SCN-BRZ-03.3 — an fsspec in-memory bucket stands in for s3://idp-landing
    import fsspec
    landing.write_landing(multi_tenant_dataset, tmp_path)
    landing.write_landing(multi_tenant_dataset, "memory://idp-landing")
    mem = fsspec.filesystem("memory")
    local = {p.relative_to(tmp_path).as_posix(): p.read_bytes() for p in tmp_path.rglob("*.json")}
    remote = {p.split("idp-landing/", 1)[1]: mem.cat_file(p) for p in mem.find("/idp-landing")}
    assert remote == local and local
    mem.rm("/idp-landing", recursive=True)


# --- reading the landing zone for publishing (REQ-STR-01) ----------------------
def _read(root, tenants=("tenant_a", "tenant_b")):
    return list(landing.read_landing(root, tenants))


def test_read_landing_round_trip_in_publish_order(multi_tenant_dataset, tmp_path):
    import json
    landing.write_landing(multi_tenant_dataset, tmp_path)
    files = _read(tmp_path)
    assert all(f.reason is None for f in files)
    assert [f.tenant for f in files] == sorted(f.tenant for f in files)          # tenant by tenant
    for tenant in ("tenant_a", "tenant_b"):
        mine = [f for f in files if f.tenant == tenant]
        assert [f.entity for f in mine] == [e for e in ENTITIES if any(m.entity == e for m in mine)]
        for f in mine:
            on_disk = json.loads((tmp_path / f"tenant={tenant}" / f"{f.entity}.json").read_text())
            assert f.records == on_disk


def _write(path, payload):
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload if isinstance(payload, str) else json.dumps(payload))


def test_read_landing_rejects_bad_files_and_keeps_good_ones(tmp_path):
    good = [{"tenant_id": "tenant_a", "id": "1"}]
    _write(tmp_path / "tenant=tenant_a" / "parties.json", good)
    _write(tmp_path / "tenant=tenant_a" / "recharge_events.json",               # SCN-STR-01.4
           [{"tenant_id": "tenant_a", "id": "r1"}, {"tenant_id": "tenant_b", "id": "r2"}])
    _write(tmp_path / "tenant=tenant_a" / "notes.json", good)                   # unknown entity
    _write(tmp_path / "tenant=tenant_a" / "accounts.json", "{not json")         # invalid json
    _write(tmp_path / "tenant=tenant_a" / "products.json", {"tenant_id": "tenant_a"})   # not an array
    _write(tmp_path / "tenant=tenant_x" / "parties.json", [{"tenant_id": "tenant_x"}])  # SCN-STR-01.5
    _write(tmp_path / "stray.json", good)                                       # not a tenant folder
    by_name = {(f.tenant, f.entity or f.path.rsplit("/", 1)[-1]): f for f in _read(tmp_path)}

    assert by_name[("tenant_a", "parties")].records == good
    reasons = {k: f.reason for k, f in by_name.items() if f.reason}
    assert reasons[("tenant_a", "recharge_events")].startswith("tenant mismatch: 1")
    assert by_name[("tenant_a", "recharge_events")].records is None           # never partly returned
    assert reasons[("tenant_a", "notes")].startswith("unknown entity")
    assert reasons[("tenant_a", "accounts")].startswith("invalid json")
    assert reasons[("tenant_a", "products")].startswith("invalid json")
    assert reasons[("tenant_x", "parties")].startswith("unknown tenant")
    assert reasons[("", "stray.json")] == "not a tenant folder"


def test_read_landing_missing_zone_yields_nothing(tmp_path):
    assert _read(tmp_path / "absent") == []
