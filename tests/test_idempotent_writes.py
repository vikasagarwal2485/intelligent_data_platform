"""Idempotent stage writes (REQ-SLV-06, REQ-GLD-04, REQ-ML-08, REQ-SRV-05).

Re-running a stage replaces the tenant partitions it writes instead of stacking
copies; score tables are the exception (append-only history), and publish serves
only the latest score run, one row per (tenant_id, service_id).
"""
from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine

from idp.common import storage
from idp.common.config import PlatformConfig, StorageConfig, Tenant
from idp.ml.credit_model import CreditLimitModel
from idp.pipeline import (
    ENTITIES, _read_labels, latest_score_run, publish_serving, to_bronze, to_gold,
    to_labels, to_scores, to_silver,
)
from idp.serving import timescale
from idp.synthetic.generator import generate_for_tenants

KEY = ["tenant_id", "service_id"]


def _cfg(tmp_path) -> PlatformConfig:
    sc = StorageConfig(
        root=str(tmp_path), landing=str(tmp_path / "l"), bronze=str(tmp_path / "b"),
        silver=str(tmp_path / "s"), gold=str(tmp_path / "g"), ml_artifacts=str(tmp_path / "ml"),
    )
    tenants = [Tenant("tenant_a", "A", "eu", "salt-a"), Tenant("tenant_b", "B", "eu", "salt-b")]
    return PlatformConfig(raw={"s3": {"enabled": False}, "features": {"windows": [30, 90]}},
                          tenants=tenants, storage=sc)


def _read(cfg, layer, table) -> pd.DataFrame:
    df = storage.read_parquet(f"{storage.lake_uri(cfg, layer)}/{table}", cfg)
    df["tenant_id"] = df["tenant_id"].astype(str)
    return df


@pytest.fixture(scope="module")
def dataset():
    return generate_for_tenants(["tenant_a", "tenant_b"], subscribers=40, days=30,
                                fraud_rate=0.2, seed=7)


@pytest.fixture()
def frames(dataset):
    return {e: getattr(dataset, e) for e in ENTITIES}


# --- 1. storage write modes --------------------------------------------------
def test_replace_is_idempotent_and_append_stacks(tmp_path):
    cfg = _cfg(tmp_path)
    df = pd.DataFrame({"tenant_id": ["tenant_a", "tenant_b"], "x": [1, 2]})
    for _ in range(3):
        storage.write_parquet(df, str(tmp_path / "r"), cfg, partition_cols=["tenant_id"])
        storage.write_parquet(df, str(tmp_path / "a"), cfg, partition_cols=["tenant_id"],
                              mode="append")
    assert len(storage.read_parquet(str(tmp_path / "r"), cfg)) == 2
    assert len(storage.read_parquet(str(tmp_path / "a"), cfg)) == 6


def test_replace_leaves_other_tenants_untouched(tmp_path):
    # SCN-SLV-06.2
    cfg = _cfg(tmp_path)
    uri = str(tmp_path / "t")
    storage.write_parquet(pd.DataFrame({"tenant_id": ["tenant_a", "tenant_b"], "x": [1, 2]}),
                          uri, cfg, partition_cols=["tenant_id"])
    storage.write_parquet(pd.DataFrame({"tenant_id": ["tenant_a"], "x": [9]}),
                          uri, cfg, partition_cols=["tenant_id"])
    got = storage.read_parquet(uri, cfg)
    got["tenant_id"] = got["tenant_id"].astype(str)
    assert dict(zip(got["tenant_id"], got["x"])) == {"tenant_a": 9, "tenant_b": 2}


def test_unknown_mode_rejected(tmp_path):
    with pytest.raises(ValueError, match="write mode"):
        storage.write_parquet(pd.DataFrame({"tenant_id": ["a"]}), str(tmp_path / "x"),
                              _cfg(tmp_path), partition_cols=["tenant_id"], mode="upsert")


# --- 2. stage callers --------------------------------------------------------
def test_silver_gold_labels_rerun_do_not_duplicate(tmp_path, frames, dataset):
    # SCN-SLV-06.1, SCN-GLD-04.1, SCN-ML-08.1
    cfg = _cfg(tmp_path)
    to_bronze(cfg, frames)
    counts = []
    for _ in range(2):
        silver = to_silver(cfg)
        to_gold(cfg)
        to_labels(cfg, dataset=dataset)
        counts.append({
            **{t: len(_read(cfg, "silver", t)) for t in silver},
            "credit_features": len(_read(cfg, "gold", "credit_features")),
            "fraud_features": len(_read(cfg, "gold", "fraud_features")),
            "labels": len(storage.read_parquet(storage.labels_uri(cfg), cfg)),
        })
    assert counts[0] == counts[1]
    assert not _read(cfg, "silver", "dim_subscriber").duplicated(KEY).any()
    assert not _read(cfg, "gold", "credit_features").duplicated(KEY).any()
    assert not _read_labels(cfg).duplicated(KEY).any()
    assert counts[1]["labels"] == len(dataset.labels)


def test_bronze_stays_append_only(tmp_path, frames):
    # Bronze is a raw log: a second landed batch adds to it (Silver de-duplicates).
    cfg = _cfg(tmp_path)
    to_bronze(cfg, frames)
    to_bronze(cfg, frames)
    assert len(_read(cfg, "bronze", "parties")) == 2 * len(frames["parties"])
    assert not to_silver(cfg, persist=False)["dim_subscriber"].duplicated(KEY).any()


def test_score_runs_append_one_row_per_subscriber(tmp_path, frames, dataset):
    # SCN-ML-08.2
    from idp.pipeline import train
    cfg = _cfg(tmp_path)
    silver = to_silver(cfg, frames, persist=False)
    gold = to_gold(cfg, silver=silver, persist=False)
    models = train(cfg, dataset=dataset, silver=silver, persist=False)["models"]
    for _ in range(2):
        to_scores(cfg, gold=gold, models=models)
    scores = _read(cfg, "gold", "credit_scores")
    assert scores["scored_at"].nunique() == 2
    for _, run in scores.groupby("scored_at"):
        assert len(run) == len(gold["credit_features"])
        assert not run.duplicated(KEY).any()


def test_gold_refresh_after_changed_silver_leaves_no_stale_rows(tmp_path, frames):
    # SCN-GLD-04.2
    cfg = _cfg(tmp_path)
    silver = to_silver(cfg, frames, persist=False)
    to_gold(cfg, silver=silver)
    dim = silver["dim_subscriber"]
    keep = dim.groupby("tenant_id").head(10)          # a smaller Silver for the refresh
    to_gold(cfg, silver={**silver, "dim_subscriber": keep})
    gold = _read(cfg, "gold", "credit_features")
    assert set(map(tuple, gold[KEY].values)) == set(map(tuple, keep[KEY].values))


# --- 3. publish latest + uniqueness ------------------------------------------
def _scores(run_ts: str, limit: float) -> pd.DataFrame:
    return pd.DataFrame([
        {"tenant_id": t, "service_id": s, "recommended_limit": limit,
         "scored_at": pd.Timestamp(run_ts, tz="UTC")}
        for t in ("tenant_a", "tenant_b") for s in ("tok_1", "tok_2")
    ])


def _serving_cfg(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.raw["timescale"] = {"schema": None}
    return cfg


def test_latest_score_run_per_tenant():
    old, new = _scores("2026-09-01", 10.0), _scores("2026-09-02", 20.0)
    # tenant_b only has the old run: its latest is still that run
    both = pd.concat([old, new[new["tenant_id"] == "tenant_a"]], ignore_index=True)
    got = latest_score_run(both)
    assert dict(got.groupby("tenant_id")["recommended_limit"].first()) == {
        "tenant_a": 20.0, "tenant_b": 10.0}
    assert not got.duplicated(KEY).any()


def test_publish_serves_latest_run_only(tmp_path, monkeypatch):
    # SCN-SRV-05.1 and task 3.3 (read_score returns the latest-run row)
    cfg = _serving_cfg(tmp_path)
    engine = create_engine(f"sqlite:///{tmp_path}/serving.db")
    monkeypatch.setattr(timescale, "get_engine", lambda _cfg: engine)
    gold = {"credit_scores": pd.concat([_scores("2026-09-01", 10.0), _scores("2026-09-02", 20.0)],
                                       ignore_index=True)}
    publish_serving(cfg, gold)
    served = pd.read_sql("SELECT * FROM credit_scores", engine)
    assert len(served) == 4 and not served.duplicated(KEY).any()
    assert set(served["recommended_limit"]) == {20.0}
    row = timescale.read_score(cfg, "credit_scores", "tenant_a", "tok_1", engine=engine)
    assert row["recommended_limit"] == 20.0


def _transactional_sqlite(path):
    """SQLite engine with real transactional DDL (as on Postgres).

    pysqlite auto-commits DDL by default, so a failed replace would still drop the
    table; SQLAlchemy's documented recipe makes BEGIN cover DDL too.
    """
    from sqlalchemy import event

    engine = create_engine(f"sqlite:///{path}")

    @event.listens_for(engine, "connect")
    def _no_pysqlite_tx(dbapi_conn, _record):
        dbapi_conn.isolation_level = None

    @event.listens_for(engine, "begin")
    def _begin(conn):
        conn.exec_driver_sql("BEGIN")

    return engine


def test_duplicate_keys_fail_and_keep_previous_table(tmp_path):
    # SCN-SRV-05.2
    engine = _transactional_sqlite(tmp_path / "serving.db")
    good = _scores("2026-09-01", 10.0)
    timescale.publish_frame(engine, "credit_scores", good)
    dup = pd.concat([_scores("2026-09-02", 20.0)] * 2, ignore_index=True)
    with pytest.raises(Exception):
        timescale.publish_frame(engine, "credit_scores", dup)
    served = pd.read_sql("SELECT * FROM credit_scores", engine)
    assert len(served) == len(good) and set(served["recommended_limit"]) == {10.0}
