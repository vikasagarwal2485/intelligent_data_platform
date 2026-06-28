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
