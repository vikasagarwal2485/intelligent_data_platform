"""End-to-end pipeline test (plan 3-6).

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
    return tmp_path, summary


def test_pipeline_persists_gold_and_artifacts(pipeline_result):
    tmp_path, summary = pipeline_result
    assert summary["counts"]["subscribers"] == 600  # 2 tenants x 300
    # Gold tables are tenant-partitioned directories (one per table).
    assert (tmp_path / "gold" / "credit_features").is_dir()
    assert (tmp_path / "gold" / "fraud_features").is_dir()
    assert (tmp_path / "gold" / "credit_scores").is_dir()
    assert (tmp_path / "gold" / "fraud_scores").is_dir()
    # Silver + Bronze layers were written by their stages too.
    assert (tmp_path / "silver" / "dim_subscriber").is_dir()
    assert (tmp_path / "bronze" / "parties").is_dir()
    # Trained models saved by the training stage.
    assert (tmp_path / "ml" / "credit_model.joblib").exists()
    assert (tmp_path / "ml" / "fraud_model.joblib").exists()


def test_pipeline_credit_model_beats_random(pipeline_result):
    _, summary = pipeline_result
    # out-of-sample credit PD AUC is realistic for a credit scorecard (>0.6)
    assert summary["credit_metrics"]["auc"] > 0.6
    assert summary["credit_metrics"]["ks"] > 0.1


def test_pipeline_fraud_model_beats_base_rate(pipeline_result):
    _, summary = pipeline_result
    fm = summary["fraud_metrics"]
    assert fm["pr_auc"] > fm["fraud_rate"]
    assert fm["pr_auc"] > 0.7


def test_pipeline_scores_cover_all_subscribers(pipeline_result):
    _, summary = pipeline_result
    n = summary["counts"]["subscribers"]
    assert len(summary["credit_scores"]) == n
    assert len(summary["fraud_scores"]) == n


def test_pipeline_is_tenant_isolated(pipeline_result):
    _, summary = pipeline_result
    assert set(summary["credit_scores"]["tenant_id"]) == {"tenant_a", "tenant_b"}
    assert set(summary["fraud_scores"]["tenant_id"]) == {"tenant_a", "tenant_b"}
