"""Iceberg Bronze landing (REQ-BRZ-01)."""
from __future__ import annotations

import pytest

from conftest import lake_cfg
from idp.common import catalog
from idp.ingestion.raw import bronze_tables, raw_events
from idp.pipeline import to_bronze
from idp.synthetic.generator import generate_for_tenants


@pytest.fixture(scope="module")
def frames():
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=5, days=3, seed=2)
    return raw_events(ds)                         # raw TMF event rows per table


def test_write_and_reload_bronze(tmp_path, frames):
    # SCN-BRZ-01.1
    cfg = lake_cfg(tmp_path)
    result = to_bronze(cfg, frames)
    assert set(result["bronze_tables"]) == set(bronze_tables().values())
    got = catalog.read_table(cfg, "bronze", "tmf_party")
    assert len(got) == len(frames["tmf_party"])
    assert set(got["tenant_id"]) == {"tenant_a", "tenant_b"}
    assert got["_ingested_at"].notna().all() and got["ingest_date"].notna().all()


def test_partitioned_by_tenant_and_ingest_date(tmp_path, frames):
    # SCN-BRZ-01.2
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, frames)
    t = catalog.load_catalog(cfg).load_table("bronze.tmf_product_usage")
    assert [f.name for f in t.spec().fields] == ["tenant_id", "ingest_date"]
    from pyiceberg.expressions import EqualTo
    files = list(t.scan(row_filter=EqualTo("tenant_id", "tenant_a")).plan_files())
    assert files and all("tenant_id=tenant_a" in f.file.file_path for f in files)


def test_append_only_history(tmp_path, frames):
    # SCN-BRZ-01.3 — a second (overlapping) batch is a new snapshot; nothing rewritten
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, frames)
    first = catalog.read_table(cfg, "bronze", "tmf_topup_balance")
    to_bronze(cfg, {"tmf_topup_balance": frames["tmf_topup_balance"]})
    t = catalog.load_catalog(cfg).load_table("bronze.tmf_topup_balance")
    assert [s.summary.operation.value for s in t.snapshots()] == ["append", "append"]
    both = catalog.read_table(cfg, "bronze", "tmf_topup_balance")
    assert len(both) == 2 * len(first)
    # every record of the first batch is still there unchanged
    key = ["tenant_id", "event_id", "kafka_offset", "_ingested_at"]
    assert first[key].merge(both[key], on=key).shape[0] == len(first)


def test_empty_frames_land_nothing(tmp_path):
    cfg = lake_cfg(tmp_path)
    import pandas as pd
    assert to_bronze(cfg, {"tmf_party": pd.DataFrame()})["bronze_tables"] == []
    assert catalog.table_exists(cfg, "bronze", "tmf_party") is False
