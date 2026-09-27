"""Iceberg Bronze landing (REQ-BRZ-01)."""
from __future__ import annotations

import pytest

from conftest import lake_cfg
from idp.common import catalog
from idp.pipeline import ENTITIES, to_bronze
from idp.synthetic.generator import generate_for_tenants


@pytest.fixture(scope="module")
def frames():
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=5, days=3, seed=2)
    return {e: getattr(ds, e) for e in ENTITIES}


def test_write_and_reload_bronze(tmp_path, frames):
    # SCN-BRZ-01.1
    cfg = lake_cfg(tmp_path)
    result = to_bronze(cfg, frames)
    assert set(result["bronze_tables"]) == set(ENTITIES)
    got = catalog.read_table(cfg, "bronze", "parties")
    assert len(got) == len(frames["parties"])
    assert set(got["tenant_id"]) == {"tenant_a", "tenant_b"}
    assert got["_ingested_at"].notna().all() and got["ingest_date"].notna().all()


def test_partitioned_by_tenant_and_ingest_date(tmp_path, frames):
    # SCN-BRZ-01.2
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, frames)
    t = catalog.load_catalog(cfg).load_table("bronze.voice_events")
    assert [f.name for f in t.spec().fields] == ["tenant_id", "ingest_date"]
    from pyiceberg.expressions import EqualTo
    files = list(t.scan(row_filter=EqualTo("tenant_id", "tenant_a")).plan_files())
    assert files and all("tenant_id=tenant_a" in f.file.file_path for f in files)


def test_append_only_history(tmp_path, frames):
    # SCN-BRZ-01.3 — a second (overlapping) batch is a new snapshot; nothing rewritten
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, frames)
    first = catalog.read_table(cfg, "bronze", "recharge_events")
    to_bronze(cfg, {"recharge_events": frames["recharge_events"]})
    t = catalog.load_catalog(cfg).load_table("bronze.recharge_events")
    assert [s.summary.operation.value for s in t.snapshots()] == ["append", "append"]
    both = catalog.read_table(cfg, "bronze", "recharge_events")
    assert len(both) == 2 * len(first)
    # every record of the first batch is still there unchanged
    key = ["tenant_id", "id", "_ingested_at"]
    assert first[key].merge(both[key], on=key).shape[0] == len(first)


def test_empty_frames_land_nothing(tmp_path):
    cfg = lake_cfg(tmp_path)
    import pandas as pd
    assert to_bronze(cfg, {"parties": pd.DataFrame()})["bronze_tables"] == []
    assert catalog.table_exists(cfg, "bronze", "parties") is False
