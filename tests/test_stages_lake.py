"""Stages compose through the lake on the single Spark engine (REQ-ORC-03/05,
REQ-SLV-04, REQ-GLD-05, REQ-ML-03)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from golden_data import golden_cfg, land_golden_bronze
from idp.common import storage
from idp.pipeline import ENTITIES, to_bronze, to_gold, to_labels, to_scores, to_silver, train
from idp.synthetic.generator import generate_for_tenants

KEY = ["tenant_id", "service_id"]


def _read(cfg, layer, table) -> pd.DataFrame:
    from idp.common import catalog
    return catalog.read_table(cfg, layer, table)


def _consenting(cfg) -> int:
    """Subscribers with analytics consent — the ones that get Gold rows (REQ-FND-05)."""
    dim = _read(cfg, "silver", "dim_subscriber")
    return int(dim["consent_analytics"].fillna(False).astype(bool).sum())


# --- REQ-SLV-04 ---------------------------------------------------------------
def test_silver_fails_clearly_without_core_bronze(spark, tmp_path):
    # SCN-SLV-04.1
    cfg = golden_cfg(tmp_path)
    ds = generate_for_tenants(["tenant_a"], subscribers=5, days=3, seed=1)
    to_bronze(cfg, {"voice_events": ds.voice_events})          # no parties/accounts/agreements
    with pytest.raises(RuntimeError, match="Populate Bronze first"):
        to_silver(cfg)


def test_silver_reads_the_bronze_the_landing_wrote(spark, tmp_path):
    # SCN-SLV-04.2 — Silver built as a separate step from the persisted Bronze
    cfg = golden_cfg(tmp_path)
    land_golden_bronze(cfg)
    summary = to_silver(cfg)["silver_tables"]
    assert summary["dim_subscriber"] == 40                     # 2 tenants x 20 subscribers
    assert summary["dim_subscriber"] == len(_read(cfg, "silver", "dim_subscriber"))
    assert set(summary) == {"dim_subscriber", "fact_usage", "fact_recharge", "fact_fee_charge"}


# --- REQ-GLD-05 ---------------------------------------------------------------
def test_gold_built_from_persisted_silver(spark, tmp_path):
    # SCN-GLD-05.1
    cfg = golden_cfg(tmp_path)
    land_golden_bronze(cfg)
    to_silver(cfg)
    result = to_gold(cfg)
    n = _consenting(cfg)
    assert 0 < n <= 40 and result["counts"]["subscribers"] == n
    for table in result["gold_tables"]:
        df = _read(cfg, "gold", table)
        assert len(df) == n and not df.duplicated(KEY).any()


def test_gold_fails_clearly_without_silver(spark, tmp_path):
    # SCN-GLD-05.2
    cfg = golden_cfg(tmp_path)
    with pytest.raises(RuntimeError, match="Run the Silver stage"):
        to_gold(cfg)
    from idp.common import catalog
    assert not catalog.load_catalog(cfg).list_tables("gold")


# --- REQ-ORC-05: no whole-table pandas in Silver/Gold ----------------------------
def test_silver_and_gold_never_convert_to_pandas(spark, tmp_path, monkeypatch):
    # SCN-ORC-05.3
    from pyspark.sql import DataFrame

    def forbidden(*_a, **_k):
        raise AssertionError("Silver/Gold must not materialize a table into pandas")

    cfg = golden_cfg(tmp_path)
    land_golden_bronze(cfg)                                    # the Bronze landing is pandas (by design)
    monkeypatch.setattr(DataFrame, "toPandas", forbidden)
    to_silver(cfg)
    to_gold(cfg)


def test_non_transform_entrypoints_do_not_load_pyspark():
    # task 3.6 — importing the API/MCP/ML stages must not pull in pyspark / start a JVM
    code = ("import sys; import idp.serving.api, idp.mcp.tools, idp.pipeline, idp.cli; "
            "print('pyspark' in sys.modules)")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


# --- REQ-ML-03 / REQ-ORC-03 ------------------------------------------------------
@pytest.fixture(scope="module")
def local_lake(spark, tmp_path_factory):
    """A full local lake built by the separate stages (Bronze -> Silver -> labels -> Gold)."""
    cfg = golden_cfg(tmp_path_factory.mktemp("lake"))
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=60, days=30, fraud_rate=0.2,
                              default_rate=0.2, seed=7)
    to_bronze(cfg, {e: getattr(ds, e) for e in ENTITIES})
    to_silver(cfg)
    to_labels(cfg, dataset=ds)
    to_gold(cfg)
    return cfg


def test_train_reads_only_gold_and_labels(local_lake, monkeypatch):
    # SCN-ML-03.3 / SCN-ML-03.4 — the local lake, never Silver, never regenerating
    import idp.pipeline as pipeline
    from idp.transforms import lake
    read_layers = []
    real = lake.read_table                        # training reads the lake with Spark
    monkeypatch.setattr(lake, "read_table",
                        lambda spark, cfg, layer, table: read_layers.append(layer) or real(spark, cfg, layer, table))
    monkeypatch.setattr(pipeline, "_gen_dataset", lambda *_: pytest.fail("must not regenerate data"))
    result = train(local_lake, persist=True)
    assert set(read_layers) == {"gold"}
    assert result["credit_metrics"]["n"] > 0
    assert result["credit_metrics"]["sampled_rows"] > 0 and "sample_seed" in result["credit_metrics"]
    assert storage.model_exists(storage.model_uri(local_lake, "credit_model.joblib"), local_lake)


def test_train_registers_and_score_uses_the_champion(local_lake, tmp_path, monkeypatch):
    # REQ-ML-04 with a registry (SCN-ML-04.2/04.3): train registers both versions,
    # moves `champion` per gate, and Score (no in-memory models) uses the champions
    monkeypatch.delenv("IDP_MLFLOW_TRACKING_URI", raising=False)
    import copy
    from idp.ml.registry import model_store
    cfg = copy.copy(local_lake)
    cfg.raw = {**local_lake.raw, "mlflow": {
        "tracking_uri": f"sqlite:///{tmp_path / 'mlflow.db'}", "experiment": "idp-train-test",
        "artifact_location": (tmp_path / "mlartifacts").as_uri()},
        "credit": {**local_lake.raw.get("credit", {}), "min_auc": 0.0},
        "fraud": {**local_lake.raw.get("fraud", {}), "min_pr_auc_lift": 0.0}}
    result = train(cfg, persist=True)
    reg = result["registry"]
    assert reg["credit"]["version"] == "1" and reg["fraud"]["version"] == "1"
    assert reg["credit"]["promoted"] and reg["fraud"]["promoted"]
    store = model_store(cfg)
    assert store.champion_version("credit") == "1"
    out = to_scores(cfg, persist=False)
    assert out["score_tables"] == ["credit_scores", "fraud_scores"]


def test_scores_read_gold_from_the_lake(local_lake):
    models = train(local_lake, persist=False)["models"]
    out = to_scores(local_lake, models=models, persist=False)
    assert out["score_tables"] == ["credit_scores", "fraud_scores"]
    assert out["counts"]["credit_scores"] == _consenting(local_lake)


def test_run_equals_separate_stages(spark, tmp_path, monkeypatch):
    # SCN-ORC-03.2 — `run` composes through the lake: its Gold == silver + gold run separately
    from idp.pipeline import run
    cfg = golden_cfg(tmp_path / "run")
    cfg.raw["synthetic"] = {"subscribers_per_tenant": 20, "days": 10, "seed": 3,
                            "fraud_rate": 0.2, "default_rate": 0.2}
    cfg.raw["credit"], cfg.raw["fraud"] = {}, {}
    summary = run(cfg)
    assert summary["counts"]["subscribers"] == 40
    for layer in ("bronze", "silver", "gold"):
        assert Path(getattr(cfg.storage, layer)).is_dir()      # every layer readable from the lake

    sep = golden_cfg(tmp_path / "separate")
    import shutil
    from conftest import clone_lake
    sep = clone_lake(cfg, tmp_path / "separate", layers=("bronze",))   # same Bronze
    to_silver(sep)
    to_gold(sep)
    for table in ("credit_features", "fraud_features"):
        a = _read(cfg, "gold", table).sort_values(KEY).reset_index(drop=True)
        b = _read(sep, "gold", table).sort_values(KEY).reset_index(drop=True)
        pd.testing.assert_frame_equal(a, b, check_dtype=False, check_categorical=False)


def test_run_without_persist_uses_a_temporary_lake(spark, tmp_path):
    from idp.pipeline import run
    cfg = golden_cfg(tmp_path / "lake")
    cfg.raw["synthetic"] = {"subscribers_per_tenant": 20, "days": 10, "seed": 3,
                            "fraud_rate": 0.2, "default_rate": 0.2}
    summary = run(cfg, persist=False)
    assert summary["counts"]["subscribers"] == 40
    assert not (tmp_path / "lake").exists()                    # nothing written to the configured lake
