"""Tests for the TimescaleDB serving layer (plan Q4/Q6).

Uses a local SQLite engine so the publish/read logic is exercised without a real
Postgres/TimescaleDB (schema-less mode).
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine

from idp.common.config import load_config
from idp.serving import timescale


def _cfg_no_schema():
    cfg = load_config()
    cfg.raw["timescale"] = {"schema": None, "enabled": True}
    return cfg


def test_make_url_from_config():
    cfg = load_config()
    cfg.raw["timescale"] = {"host": "h", "port": 5432, "database": "idp",
                            "user": "postgres", "password": "pw"}
    assert timescale.make_url(cfg) == "postgresql+psycopg2://postgres:pw@h:5432/idp"


def test_publish_and_read(tmp_path):
    cfg = _cfg_no_schema()
    engine = create_engine(f"sqlite:///{tmp_path}/serving.db")
    gold = {
        "credit_scores": pd.DataFrame([
            {"tenant_id": "tenant_a", "service_id": "tok_x", "pd": 0.2, "recommended_limit": 50.0},
            {"tenant_id": "tenant_b", "service_id": "tok_y", "pd": 0.5, "recommended_limit": 10.0},
        ]),
        "empty_table": pd.DataFrame(),
    }
    published = timescale.publish_gold(cfg, gold, engine=engine)
    assert published == ["credit_scores"]          # empty frame skipped

    row = timescale.read_score(cfg, "credit_scores", "tenant_a", "tok_x", engine=engine)
    assert row is not None and row["recommended_limit"] == 50.0

    # tenant isolation: tenant_b cannot read tenant_a's subject id
    assert timescale.read_score(cfg, "credit_scores", "tenant_b", "tok_x", engine=engine) is None


def test_read_missing_returns_none(tmp_path):
    cfg = _cfg_no_schema()
    engine = create_engine(f"sqlite:///{tmp_path}/serving2.db")
    timescale.publish_gold(cfg, {"credit_scores": pd.DataFrame([
        {"tenant_id": "tenant_a", "service_id": "tok_x", "pd": 0.1}])}, engine=engine)
    assert timescale.read_score(cfg, "credit_scores", "tenant_a", "nope", engine=engine) is None


def test_publish_without_gold_tables_fails_clearly(tmp_path):
    # SCN-SRV-01.4 — nothing to publish -> clear error pointing at the Gold stage
    import pytest

    from idp.common.config import StorageConfig
    from idp.pipeline import publish_serving
    cfg = load_config()
    cfg.storage = StorageConfig(
        root=str(tmp_path), landing=str(tmp_path / "l"), bronze=str(tmp_path / "b"),
        silver=str(tmp_path / "s"), gold=str(tmp_path / "g"), ml_artifacts=str(tmp_path / "ml"),
    )
    with pytest.raises(RuntimeError, match="Gold"):
        publish_serving(cfg)
