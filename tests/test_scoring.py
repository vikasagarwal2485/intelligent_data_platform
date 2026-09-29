"""Distributed scoring (REQ-ML-07, SCN-ML-07.5; REQ-ORC-05)."""
from __future__ import annotations

import pandas as pd
import pytest

from conftest import clone_lake
from idp.common import catalog
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.pipeline import to_scores

KEY = ["tenant_id", "service_id"]


@pytest.fixture()
def gold_lake(tmp_path, lake):
    return clone_lake(lake, tmp_path / "lake", layers=("gold",), tables={"credit_features", "fraud_features"})


@pytest.fixture()
def models(credit_labeled, fraud_labeled):
    return {"credit": CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"]),
            "fraud": FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"])}


@pytest.fixture()
def small_batches(spark):
    """Many small Arrow batches, so per-batch scoring really is partitioned."""
    key = "spark.sql.execution.arrow.maxRecordsPerBatch"
    old = spark.conf.get(key)
    spark.conf.set(key, "7")
    yield
    spark.conf.set(key, old)


def _sorted(df):
    return df.sort_values(KEY).reset_index(drop=True)


def test_distributed_scores_equal_in_process_scores(spark, gold_lake, models, small_batches):
    # SCN-ML-07.5
    out = to_scores(gold_lake, models=models, persist=False)
    credit = catalog.read_table(gold_lake, "gold", "credit_features")
    fraud = catalog.read_table(gold_lake, "gold", "fraud_features")
    expected_credit = _sorted(models["credit"].recommend_limit(credit))
    expected_fraud = _sorted(models["fraud"].score(fraud))

    got_credit = _sorted(out["credit_scores"].toPandas().drop(columns="scored_at"))
    got_fraud = _sorted(out["fraud_scores"].toPandas().drop(columns="scored_at"))
    pd.testing.assert_frame_equal(got_credit, expected_credit, check_dtype=False, rtol=1e-12)
    pd.testing.assert_frame_equal(got_fraud, expected_fraud, check_dtype=False, rtol=1e-12)
    # and the global anomaly range mattered: per-batch normalization would differ
    naive = _sorted(pd.concat(models["fraud"].score(fraud.iloc[i:i + 7]) for i in range(0, len(fraud), 7)))
    assert not naive["anomaly_score"].equals(expected_fraud["anomaly_score"])


def test_one_scored_at_per_run_and_counts(spark, gold_lake, models):
    # SCN-ML-07.4 (single run) — every row of a run carries the same scored_at
    out = to_scores(gold_lake, models=models, persist=True)
    for table in ("credit_scores", "fraud_scores"):
        stored = catalog.read_table(gold_lake, "gold", table)
        assert stored["scored_at"].nunique() == 1
        assert len(stored) == out["counts"][table]


def test_score_never_materializes_a_whole_table(spark, gold_lake, models, monkeypatch):
    # SCN-ORC-05.3 — the Score stage never converts a table to pandas on the driver
    from pyspark.sql import DataFrame

    def refuse(self, *a, **k):
        raise AssertionError("toPandas() called in the Score stage")
    monkeypatch.setattr(DataFrame, "toPandas", refuse)
    out = to_scores(gold_lake, models=models, persist=True)
    assert out["score_tables"] == ["credit_scores", "fraud_scores"]
