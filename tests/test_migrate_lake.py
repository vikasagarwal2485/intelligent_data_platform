"""``idp migrate-lake``: legacy Parquet Bronze -> Iceberg Bronze (design decision 8)."""
from __future__ import annotations

import os

import pandas as pd
import pytest

from conftest import lake_cfg
from idp.common import catalog, storage
from idp.maintenance import maintain
from idp.migration import MARKER, legacy_bronze_uri, migrate_lake
from idp.pipeline import ENTITIES, to_bronze, to_gold, to_silver
from idp.synthetic.generator import generate_for_tenants

GOLD = ("credit_features", "fraud_features")


@pytest.fixture(scope="module")
def frames():
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=20, days=20, seed=13)
    return {e: getattr(ds, e) for e in ENTITIES}


def _legacy_bronze(cfg, frames) -> None:
    """The pre-Iceberg ``to_bronze``: hive-partitioned Parquet appends, in two batches."""
    for half in (0, 1):
        for name, df in frames.items():
            storage.write_parquet(df.iloc[half::2], legacy_bronze_uri(cfg, name), cfg,
                                  partition_cols=["tenant_id"], mode="append")


def _gold(cfg) -> dict[str, pd.DataFrame]:
    return {t: catalog.read_table(cfg, "gold", t).sort_values(["tenant_id", "service_id"])
            .reset_index(drop=True) for t in GOLD}


def test_migrated_lake_rebuilds_the_same_gold(spark, tmp_path, frames):
    legacy = lake_cfg(tmp_path / "legacy")
    _legacy_bronze(legacy, frames)
    out = migrate_lake(legacy)
    assert out == {e: len(frames[e]) for e in ENTITIES}
    props = catalog.load_catalog(legacy).load_table("bronze.recharge_events").properties
    assert props[MARKER] == legacy_bronze_uri(legacy, "recharge_events")
    to_silver(legacy, full_refresh=True)
    to_gold(legacy)

    ref = lake_cfg(tmp_path / "ref")
    to_bronze(ref, frames)
    to_silver(ref)
    to_gold(ref)
    a, b = _gold(legacy), _gold(ref)
    for t in GOLD:
        pd.testing.assert_frame_equal(a[t], b[t], check_like=True)


def test_rerun_is_a_no_op_and_legacy_files_survive_maintenance(spark, tmp_path, frames):
    cfg = lake_cfg(tmp_path)
    _legacy_bronze(cfg, {e: frames[e] for e in ("parties", "recharge_events")})
    migrate_lake(cfg)
    snaps = len(catalog.load_catalog(cfg).load_table("bronze.recharge_events").snapshots())
    again = migrate_lake(cfg)
    assert again["recharge_events"] == "already migrated"
    assert again["voice_events"] == "no legacy data"
    assert len(catalog.load_catalog(cfg).load_table("bronze.recharge_events").snapshots()) == snaps
    assert len(catalog.read_table(cfg, "bronze", "recharge_events")) == len(frames["recharge_events"])
    legacy_files = sorted(os.path.join(r, f) for r, _, fs in os.walk(legacy_bronze_uri(cfg, "recharge_events"))
                          for f in fs)
    cfg.raw["lake"] = {"snapshot_retention_days": 0, "target_file_size_mb": 128}
    maintain(cfg)
    assert sorted(os.path.join(r, f) for r, _, fs in os.walk(legacy_bronze_uri(cfg, "recharge_events"))
                  for f in fs) == legacy_files


def test_each_entity_lands_as_one_snapshot_that_adds_every_file(tmp_path, frames):
    # the streaming Silver source reads a snapshot's ADDED files, so the migration
    # must add all tenants' files in the one snapshot it commits
    cfg = lake_cfg(tmp_path)
    _legacy_bronze(cfg, {"recharge_events": frames["recharge_events"]})
    migrate_lake(cfg)
    [snap] = catalog.load_catalog(cfg).load_table("bronze.recharge_events").snapshots()
    summary = snap.summary.additional_properties
    assert summary["added-data-files"] == summary["total-data-files"] != "1"
    assert int(summary["added-records"]) == len(frames["recharge_events"])
