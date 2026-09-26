"""Tests for Bronze ingestion (plan 3.2)."""
from __future__ import annotations

from idp.ingestion import bronze


def test_write_landing_partitions_by_tenant(multi_tenant_dataset, tmp_path):
    written = bronze.write_landing(multi_tenant_dataset, tmp_path)
    assert "parties" in written
    assert (tmp_path / "tenant=tenant_a" / "parties.json").exists()
    assert (tmp_path / "tenant=tenant_b" / "parties.json").exists()


def test_ingest_and_reload_roundtrip(small_dataset, tmp_path):
    frames = bronze.ingest(small_dataset, tmp_path / "bronze")
    reloaded = bronze.load_bronze(tmp_path / "bronze")
    assert set(reloaded) >= {"parties", "voice_events", "recharge_events"}
    assert len(reloaded["parties"]) == len(frames["parties"])


def test_bronze_is_partitioned_by_tenant(multi_tenant_dataset, tmp_path):
    bronze.ingest(multi_tenant_dataset, tmp_path / "bronze")
    # tenant partition directories exist on disk
    party_dir = tmp_path / "bronze" / "parties"
    parts = {p.name for p in party_dir.glob("tenant_id=*")}
    assert parts == {"tenant_id=tenant_a", "tenant_id=tenant_b"}


# --- REQ-BRZ-03: file landing zone (`idp generate`) -------------------------
def test_landing_has_one_file_per_tenant_entity(multi_tenant_dataset, tmp_path):
    import json

    bronze.write_landing(multi_tenant_dataset, tmp_path)
    for tenant_id in ("tenant_a", "tenant_b"):
        for entity in bronze.ENTITIES:
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
