"""End-to-end pipeline test.

The pipeline is expensive, so it is executed once per session via a fixture and
the individual tests assert on the shared result.
"""
from __future__ import annotations

import pytest

from idp.common.config import StorageConfig, load_config
from idp.pipeline import run


@pytest.fixture(scope="session")
def pipeline_result(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("lake")
    cfg = load_config()
    cfg.raw["synthetic"].update(
        {"subscribers_per_tenant": 300, "days": 60, "fraud_rate": 0.12, "default_rate": 0.2}
    )
    cfg.storage = StorageConfig(
        root=str(tmp_path),
        landing=str(tmp_path / "landing"),
        bronze=str(tmp_path / "bronze"),
        silver=str(tmp_path / "silver"),
        gold=str(tmp_path / "gold"),
        ml_artifacts=str(tmp_path / "ml"),
    )
    summary = run(cfg, persist=True)
    return tmp_path, summary, cfg


def test_pipeline_persists_gold_and_artifacts(pipeline_result):
    from idp.common import catalog
    tmp_path, summary, cfg = pipeline_result
    assert summary["counts"]["subscribers"] == 600  # 2 tenants x 300
    # Every layer's tables were committed to the lake's Iceberg catalog ...
    for layer, table in (("gold", "credit_features"), ("gold", "fraud_features"),
                         ("gold", "credit_scores"), ("gold", "fraud_scores"),
                         ("silver", "dim_subscriber"), ("bronze", "tmf_party")):
        assert catalog.table_exists(cfg, layer, table), (layer, table)
        # ... with their data under the layer's own directory
        assert catalog.table_location(cfg, layer, table).startswith((tmp_path / layer).as_uri())
    # Trained models saved by the training stage.
    assert (tmp_path / "ml" / "credit_model.joblib").exists()
    assert (tmp_path / "ml" / "fraud_model.joblib").exists()


def test_pipeline_credit_model_beats_random(pipeline_result):
    _, summary, _cfg = pipeline_result
    # out-of-sample credit PD AUC is realistic for a credit scorecard (>0.6)
    assert summary["credit_metrics"]["auc"] > 0.6
    assert summary["credit_metrics"]["ks"] > 0.1


def test_pipeline_fraud_model_beats_base_rate(pipeline_result):
    _, summary, _cfg = pipeline_result
    fm = summary["fraud_metrics"]
    assert fm["pr_auc"] > fm["fraud_rate"]
    assert fm["pr_auc"] > 0.7


def test_pipeline_scores_cover_all_consenting_subscribers(pipeline_result):
    # every subscriber with analytics consent is scored; the others never are (REQ-FND-05)
    import pandas as pd
    from idp.common import catalog
    _, summary, cfg = pipeline_result
    dim = catalog.read_table(cfg, "silver", "dim_subscriber")
    n = int(dim["consent_analytics"].fillna(False).astype(bool).sum())
    assert 0 < n < summary["counts"]["subscribers"]
    assert len(summary["credit_scores"]) == n
    assert len(summary["fraud_scores"]) == n


def test_pipeline_is_tenant_isolated(pipeline_result):
    _, summary, _cfg = pipeline_result
    assert set(summary["credit_scores"]["tenant_id"]) == {"tenant_a", "tenant_b"}
    assert set(summary["fraud_scores"]["tenant_id"]) == {"tenant_a", "tenant_b"}
