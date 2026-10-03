"""Incremental Silver on Iceberg (REQ-SLV-06/07/08)."""
from __future__ import annotations

import threading

import pandas as pd
import pytest

from conftest import lake_cfg
from idp.common import catalog
from idp.ingestion.raw import raw_events
from idp.pipeline import to_bronze, to_silver
from idp.synthetic.generator import generate_for_tenants
from idp.transforms import lake

FACTS = ("fact_usage", "fact_recharge", "fact_fee_charge")
EVENTS = ("tmf_product_usage", "tmf_topup_balance", "tmf_applied_customer_billing_rate")
REFERENCE = ("tmf_party", "tmf_customer", "tmf_billing_account", "tmf_agreement", "tmf_product")


@pytest.fixture(scope="module")
def batches():
    """Raw Bronze rows: the reference events in batch 1; the fact events split into 3
    batches by row order."""
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=15, days=9, fraud_rate=0.2, seed=5)
    raw = raw_events(ds)
    out = [{t: raw[t] for t in REFERENCE}, {}, {}]
    for e in EVENTS:
        df = raw[e].reset_index(drop=True)
        thirds = [df.iloc[i::3] for i in range(3)]
        for i in range(3):
            out[i][e] = thirds[i]
    return out


def _facts(cfg) -> dict[str, pd.DataFrame]:
    return {f: catalog.read_table(cfg, "silver", f).sort_values(["tenant_id", "id"]).reset_index(drop=True)
            for f in FACTS}


def _snapshots(cfg, table) -> int:
    return len(catalog.load_catalog(cfg).load_table(f"silver.{table}").snapshots())


def test_only_new_bronze_is_processed(spark, tmp_path, batches):
    # SCN-SLV-07.1
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, batches[0])
    first = to_silver(cfg)["processed"]
    to_bronze(cfg, batches[1])
    second = to_silver(cfg)["processed"]
    assert first["fact_recharge"] == len(batches[0]["tmf_topup_balance"])
    assert second["fact_recharge"] == len(batches[1]["tmf_topup_balance"])      # only batch 2
    assert second["fact_usage"] == len(batches[1]["tmf_product_usage"])
    total = len(batches[0]["tmf_topup_balance"]) + len(batches[1]["tmf_topup_balance"])
    assert len(catalog.read_table(cfg, "silver", "fact_recharge")) == total


def test_nothing_new_changes_nothing(spark, tmp_path, batches):
    # SCN-SLV-07.2 and SCN-SLV-06.1
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, batches[0])
    to_silver(cfg)
    before = {f: _snapshots(cfg, f) for f in FACTS}
    counts = _facts(cfg)
    again = to_silver(cfg)
    assert again["processed"] == {f: 0 for f in FACTS}
    assert {f: _snapshots(cfg, f) for f in FACTS} == before
    assert {f: len(d) for f, d in _facts(cfg).items()} == {f: len(d) for f, d in counts.items()}
    dim = catalog.read_table(cfg, "silver", "dim_subscriber")
    assert not dim.duplicated(["tenant_id", "service_id"]).any()


def test_incremental_equals_full_rebuild(spark, tmp_path, batches):
    # SCN-SLV-07.3 — 3 batches with Silver after each == one run over the same Bronze
    inc, full = lake_cfg(tmp_path / "inc"), lake_cfg(tmp_path / "full")
    for b in batches:
        to_bronze(inc, b)
        to_silver(inc)
        to_bronze(full, b)
    to_silver(full)
    a, b = _facts(inc), _facts(full)
    for f in FACTS:
        pd.testing.assert_frame_equal(a[f], b[f], check_like=True)
    dims = [catalog.read_table(c, "silver", "dim_subscriber").sort_values(["tenant_id", "service_id"])
            .reset_index(drop=True) for c in (inc, full)]
    pd.testing.assert_frame_equal(dims[0], dims[1], check_like=True)


def test_failed_run_resumes_without_loss_or_duplicates(spark, tmp_path, batches, monkeypatch):
    # SCN-SLV-07.4
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, batches[0])
    to_silver(cfg)
    to_bronze(cfg, batches[1])
    real = lake.merge_insert_new
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:                      # fail mid-run, after reading new Bronze
            raise RuntimeError("simulated failure before commit")
        return real(*args, **kwargs)

    monkeypatch.setattr(lake, "merge_insert_new", flaky)
    with pytest.raises(RuntimeError):
        to_silver(cfg)
    monkeypatch.setattr(lake, "merge_insert_new", real)
    to_silver(cfg)                               # resumes from the checkpoints

    ref = lake_cfg(tmp_path / "ref")
    to_bronze(ref, batches[0])
    to_bronze(ref, batches[1])
    to_silver(ref)
    a, b = _facts(cfg), _facts(ref)
    for f in FACTS:
        assert not a[f].duplicated(["tenant_id", "id"]).any()
        pd.testing.assert_frame_equal(a[f], b[f], check_like=True)


def test_full_refresh_rebuilds_and_later_runs_stay_incremental(spark, tmp_path, batches):
    # SCN-SLV-08.1
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, batches[0])
    to_silver(cfg)
    to_bronze(cfg, batches[1])
    to_silver(cfg)
    before = _facts(cfg)
    refreshed = to_silver(cfg, full_refresh=True)
    expected_recharges = len(batches[0]["tmf_topup_balance"]) + len(batches[1]["tmf_topup_balance"])
    assert refreshed["processed"]["fact_recharge"] == expected_recharges     # all of Bronze again
    after = _facts(cfg)
    for f in FACTS:
        pd.testing.assert_frame_equal(before[f], after[f], check_like=True)
    to_bronze(cfg, batches[2])
    assert to_silver(cfg)["processed"]["fact_recharge"] == len(batches[2]["tmf_topup_balance"])


def test_readers_never_see_a_partial_write(spark, tmp_path, batches):
    # SCN-SLV-06.3 — a reader scanning while Silver commits always gets a complete snapshot
    cfg = lake_cfg(tmp_path)
    to_bronze(cfg, batches[0])
    to_silver(cfg)
    to_bronze(cfg, batches[1])
    to_bronze(cfg, batches[2])
    valid = {len(catalog.read_table(cfg, "silver", "fact_recharge"))}
    seen, errors, done = [], [], threading.Event()

    def reader():
        while not done.is_set():
            try:
                seen.append(len(catalog.read_table(cfg, "silver", "fact_recharge")))
            except Exception as e:  # noqa: BLE001
                errors.append(e)

    t = threading.Thread(target=reader)
    t.start()
    try:
        to_silver(cfg)
    finally:
        done.set()
        t.join()
    final = len(catalog.read_table(cfg, "silver", "fact_recharge"))
    assert not errors
    assert seen and set(seen) <= valid | {final}      # before or after the commit only
