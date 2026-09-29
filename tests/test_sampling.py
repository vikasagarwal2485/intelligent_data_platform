"""Bounded training sets (REQ-ML-09)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from idp.transforms.sampling import quotas, stratified_cap


def test_quotas_sum_exactly_to_cap_and_keep_shares():
    counts = {("a", 0): 44_000, ("a", 1): 6_000, ("b", 0): 47_500, ("b", 1): 2_500}
    q = quotas(counts, 20_000)
    assert sum(q.values()) == 20_000
    for k, n in counts.items():
        assert abs(q[k] - n * 20_000 / 100_000) <= 1          # proportional, +-1 from rounding
    assert quotas({"x": 5}, 10) == {"x": 5}                   # under the cap: everything


def _labelled(spark, rows=100_000, seed=0):
    rng = np.random.default_rng(seed)
    tenant = np.where(rng.random(rows) < 0.5, "tenant_a", "tenant_b")
    default_rate = np.where(tenant == "tenant_a", 0.12, 0.05)
    return spark.createDataFrame(pd.DataFrame({
        "tenant_id": tenant, "service_id": [f"s{i}" for i in range(rows)],
        "will_default": (rng.random(rows) < default_rate).astype(int)}))


def _rates(pdf):
    return pdf.groupby("tenant_id")["will_default"].mean()


def test_sample_cap_applied_with_rates_kept(spark):
    # SCN-ML-09.1
    full = _labelled(spark)
    sample, rows = stratified_cap(full, ["tenant_id", "will_default"], 20_000, seed=42)
    got = sample.toPandas()
    assert rows == 20_000 and len(got) == 20_000 and not got["service_id"].duplicated().any()
    full_rates, got_rates = _rates(full.toPandas()), _rates(got)
    assert ((full_rates - got_rates).abs() < 0.01).all()       # within 1 percentage point
    again = stratified_cap(full, ["tenant_id", "will_default"], 20_000, seed=42)[0].toPandas()
    assert set(again["service_id"]) == set(got["service_id"])  # fixed seed -> same sample


def test_small_sets_unchanged(spark):
    # SCN-ML-09.2
    small = _labelled(spark, rows=500)
    out, rows = stratified_cap(small, ["tenant_id", "will_default"], 20_000, seed=42)
    assert out is small and rows == 500
