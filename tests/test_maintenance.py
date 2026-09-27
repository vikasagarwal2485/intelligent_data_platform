"""Lake maintenance (REQ-ORC-06)."""
from __future__ import annotations

import pandas as pd
import pytest

from conftest import lake_cfg
from idp.common import catalog
from idp.maintenance import maintain
from idp.pipeline import ENTITIES, to_bronze, to_silver
from idp.synthetic.generator import generate_for_tenants

REFERENCE = ("parties", "accounts", "agreements", "products")


@pytest.fixture(scope="module")
def dataset():
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=10, days=6, seed=9)
    return {e: getattr(ds, e) for e in ENTITIES}


def _cfg(tmp_path):
    cfg = lake_cfg(tmp_path)
    cfg.raw["lake"] = {"snapshot_retention_days": 0, "target_file_size_mb": 128}   # expire all old
    return cfg


def _files(cfg, ident):
    return len(list(catalog.load_catalog(cfg).load_table(ident).scan().plan_files()))


def _land_in_pieces(cfg, df, entity, pieces=5):
    for i in range(pieces):
        to_bronze(cfg, {entity: df.iloc[i::pieces]})


def test_compaction_preserves_contents(spark, tmp_path, dataset):
    # SCN-ORC-06.1 — many small files become fewer, rows unchanged
    cfg = _cfg(tmp_path)
    _land_in_pieces(cfg, dataset["recharge_events"], "recharge_events")
    before_rows = catalog.read_table(cfg, "bronze", "recharge_events")
    before_files = _files(cfg, "bronze.recharge_events")
    stats = maintain(cfg)
    after_rows = catalog.read_table(cfg, "bronze", "recharge_events")
    assert _files(cfg, "bronze.recharge_events") < before_files
    assert stats["bronze.recharge_events"]["files_after"] < stats["bronze.recharge_events"]["files_before"]
    key = ["tenant_id", "id", "_ingested_at"]
    pd.testing.assert_frame_equal(before_rows.sort_values(key).reset_index(drop=True),
                                  after_rows.sort_values(key).reset_index(drop=True), check_like=True)


def test_expiry_keeps_current_data_and_incremental_resumes(spark, tmp_path, dataset):
    # SCN-ORC-06.2 — old snapshots expire; tables still read; next Silver run reads only new Bronze
    cfg = _cfg(tmp_path)
    to_bronze(cfg, {e: dataset[e] for e in REFERENCE})
    rech = dataset["recharge_events"].reset_index(drop=True)
    first, second = rech.iloc[: len(rech) // 2], rech.iloc[len(rech) // 2:]
    _land_in_pieces(cfg, first, "recharge_events", pieces=3)
    to_silver(cfg)
    silver_before = len(catalog.read_table(cfg, "silver", "fact_recharge"))
    snaps_before = len(catalog.load_catalog(cfg).load_table("bronze.recharge_events").snapshots())
    maintain(cfg)                                            # compaction + expiry of old snapshots
    snaps_after = len(catalog.load_catalog(cfg).load_table("bronze.recharge_events").snapshots())
    assert snaps_after < snaps_before + 1                   # expired (the rewrite adds one)
    assert len(catalog.read_table(cfg, "silver", "fact_recharge")) == silver_before
    to_bronze(cfg, {"recharge_events": second})
    out = to_silver(cfg)                                    # resumes after compaction + expiry
    assert out["processed"]["fact_recharge"] == len(second)
    assert len(catalog.read_table(cfg, "silver", "fact_recharge")) == len(rech)


def test_bronze_history_is_never_deleted(spark, tmp_path, dataset):
    # SCN-ORC-06.3
    cfg = _cfg(tmp_path)
    for e in ("parties", "voice_events"):
        _land_in_pieces(cfg, dataset[e], e, pieces=3)
    before = {e: len(catalog.read_table(cfg, "bronze", e)) for e in ("parties", "voice_events")}
    maintain(cfg)
    maintain(cfg)                                           # idempotent
    assert {e: len(catalog.read_table(cfg, "bronze", e)) for e in before} == before


def test_archive_policy_renders_from_config(tmp_path):
    # REQ-ORC-06 — the Bronze archive rule follows lake.bronze_archive_days
    from idp.maintenance import archive_policy
    cfg = lake_cfg(tmp_path)
    cfg.raw["lake"] = {"bronze_archive_days": 45}
    s3 = archive_policy(cfg)["Rules"][0]
    assert s3["Filter"] == {"Prefix": "tables/"} and s3["Status"] == "Enabled"
    assert s3["Transitions"] == [{"Days": 45, "StorageClass": "GLACIER_IR"}]
    mc = archive_policy(cfg, storage_class="WARM", fmt="minio")["Rules"][0]
    assert mc["Transition"] == {"Days": 45, "StorageClass": "WARM"}
    with pytest.raises(ValueError):
        archive_policy(cfg, fmt="gcs")
