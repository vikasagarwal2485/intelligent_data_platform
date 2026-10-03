"""Lake maintenance (REQ-ORC-06)."""
from __future__ import annotations

import pandas as pd
import pytest

from conftest import lake_cfg
from idp.common import catalog
from idp.maintenance import maintain
from idp.ingestion.raw import raw_events
from idp.pipeline import to_bronze, to_silver
from idp.synthetic.generator import generate_for_tenants

REFERENCE = ("tmf_party", "tmf_customer", "tmf_billing_account", "tmf_agreement", "tmf_product")


@pytest.fixture(scope="module")
def dataset():
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=10, days=6, seed=9)
    return raw_events(ds)                       # raw Bronze rows per table


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
    _land_in_pieces(cfg, dataset["tmf_topup_balance"], "tmf_topup_balance")
    before_rows = catalog.read_table(cfg, "bronze", "tmf_topup_balance")
    before_files = _files(cfg, "bronze.tmf_topup_balance")
    stats = maintain(cfg)
    after_rows = catalog.read_table(cfg, "bronze", "tmf_topup_balance")
    assert _files(cfg, "bronze.tmf_topup_balance") < before_files
    assert stats["bronze.tmf_topup_balance"]["files_after"] < stats["bronze.tmf_topup_balance"]["files_before"]
    key = ["tenant_id", "event_id", "kafka_offset", "_ingested_at"]
    pd.testing.assert_frame_equal(before_rows.sort_values(key).reset_index(drop=True),
                                  after_rows.sort_values(key).reset_index(drop=True), check_like=True)


def test_expiry_keeps_current_data_and_incremental_resumes(spark, tmp_path, dataset):
    # SCN-ORC-06.2 — old snapshots expire; tables still read; next Silver run reads only new Bronze
    cfg = _cfg(tmp_path)
    to_bronze(cfg, {e: dataset[e] for e in REFERENCE})
    rech = dataset["tmf_topup_balance"].reset_index(drop=True)
    first, second = rech.iloc[: len(rech) // 2], rech.iloc[len(rech) // 2:]
    _land_in_pieces(cfg, first, "tmf_topup_balance", pieces=3)
    to_silver(cfg)
    silver_before = len(catalog.read_table(cfg, "silver", "fact_recharge"))
    snaps_before = len(catalog.load_catalog(cfg).load_table("bronze.tmf_topup_balance").snapshots())
    maintain(cfg)                                            # compaction + expiry of old snapshots
    snaps_after = len(catalog.load_catalog(cfg).load_table("bronze.tmf_topup_balance").snapshots())
    assert snaps_after < snaps_before + 1                   # expired (the rewrite adds one)
    assert len(catalog.read_table(cfg, "silver", "fact_recharge")) == silver_before
    to_bronze(cfg, {"tmf_topup_balance": second})
    out = to_silver(cfg)                                    # resumes after compaction + expiry
    assert out["processed"]["fact_recharge"] == len(second)
    assert len(catalog.read_table(cfg, "silver", "fact_recharge")) == len(rech)


def test_bronze_history_is_never_deleted(spark, tmp_path, dataset):
    # SCN-ORC-06.3
    cfg = _cfg(tmp_path)
    for e in ("tmf_party", "tmf_product_usage"):
        _land_in_pieces(cfg, dataset[e], e, pieces=3)
    before = {e: len(catalog.read_table(cfg, "bronze", e)) for e in ("tmf_party", "tmf_product_usage")}
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
