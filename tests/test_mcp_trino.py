"""The Trino-backed MCP tools against the pandas reference, on a real lake (REQ-MCP-06/09).

Needs the compose stack (``deploy/dev/trino-compose.yaml``) and runs with
``make mcp-trino-test``, which starts it and sets the ``IDP_*`` environment so the
whole test session (Spark, pyiceberg, Trino) uses its Postgres catalog and MinIO.
The module fixture builds a two-tenant lake through the real stages (Bronze, then
Silver on Spark) plus Gold score history, then every tool is compared with
``tests/mcp_reference.py`` on that lake:

* counts, sums, means and histogram counts are equal (to the last displayed
  digit: Trino and pandas add floats in different orders);
* percentiles and medians are exact in the tools, so they are equal too, and the
  spec's bound (within ±1% rank of the exact data) is checked as well;
* anomaly flags agree, except for rows within ±1% of the threshold.

It also checks the pushdown (tenant constraints in the IO plan, bounded rows
fetched) and the read-only identity.
"""
from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
import pytest

import mcp_reference as ref
from idp.ingestion.raw import raw_events
from idp.common.config import load_config
from idp.mcp import sql
from idp.mcp import tools

pytestmark = pytest.mark.trino

# The compose stack's host ports; the fixture wipes that lake, so it refuses to
# run against anything else (e.g. a port-forward to the cluster).
STACK = {"trino": 18080, "catalog": "127.0.0.1:15432/iceberg", "s3": "http://127.0.0.1:19000"}
TENANTS = ("tenant_a", "tenant_b")
LAYERS = ("bronze", "silver", "gold")
BIG = 10_000                       # anomaly `limit` for set comparisons (no truncation)


def _stack_cfg():
    cfg = load_config()
    if not (cfg.trino["port"] == STACK["trino"] and STACK["catalog"] in cfg.catalog["uri"]
            and cfg.s3.get("endpoint") == STACK["s3"] and cfg.catalog.get("name") == "lake"):
        pytest.skip("needs the compose Trino stack: run `make mcp-trino-test`")
    try:
        sql.query(cfg, "SELECT 1 AS tenant_id")
    except sql.LakeReadError as e:
        pytest.skip(f"compose Trino not reachable ({e}); `make trino-stack` starts it")
    return cfg


def _wipe(cfg) -> None:
    """Empty the compose lake: its buckets and every table's catalog row (the
    namespaces stay registered)."""
    import s3fs
    import sqlalchemy as sa

    fs = s3fs.S3FileSystem(key=cfg.s3["access_key"], secret=cfg.s3["secret_key"],
                           client_kwargs={"endpoint_url": cfg.s3["endpoint"]})
    for layer in LAYERS:
        bucket = f"{cfg.s3['bucket_prefix']}-{layer}"
        if fs.exists(bucket):
            fs.rm(bucket, recursive=True)
        fs.mkdir(bucket)
    engine = sa.create_engine(cfg.catalog["uri"])
    with engine.begin() as conn:
        if sa.inspect(conn).has_table("iceberg_tables"):
            conn.execute(sa.text("DELETE FROM iceberg_tables"))
    engine.dispose()


def _scores(tenant: str, n: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gold score history: ``n`` rows per table with ``scored_at`` spread over ~40
    days, so the 7/15/30-day windows differ."""
    rng = np.random.default_rng(seed)
    now = pd.Timestamp.now(tz="UTC").floor("s")
    at = [now - pd.Timedelta(days=int(d)) for d in rng.integers(0, 40, n)]
    ids = [f"tok_{tenant}_{i}" for i in range(n)]
    fraud = pd.DataFrame({
        "tenant_id": tenant, "service_id": ids,
        "fraud_proba": rng.beta(1, 6, n), "rule_score": rng.choice([0.0, 0.2, 0.4, 0.6, 1.0], n),
        "fraud_score": rng.beta(1, 5, n), "scored_at": at})
    afford = rng.gamma(4, 25, n)
    credit = pd.DataFrame({
        "tenant_id": tenant, "service_id": ids, "pd": rng.beta(2, 8, n),
        "affordability_base": afford, "recommended_limit": afford * rng.uniform(0.4, 0.95, n),
        "scored_at": at})
    return fraud, credit


@pytest.fixture(scope="module")
def lake(spark):
    """A two-tenant lake in the compose stack, built through the real stages."""
    from idp.common import catalog
    from idp.pipeline import ENTITIES, to_bronze, to_silver
    from idp.synthetic.generator import generate_for_tenants

    cfg = _stack_cfg()
    _wipe(cfg)
    ds = generate_for_tenants(list(TENANTS), subscribers=150, days=60, fraud_rate=0.2, seed=11)
    to_bronze(cfg, raw_events(ds), persist=True)
    to_silver(cfg, full_refresh=True)
    for i, t in enumerate(TENANTS):
        fraud, credit = _scores(t, 400 + 100 * i, seed=i)
        catalog.append_table(cfg, "gold", "fraud_scores", fraud)
        catalog.append_table(cfg, "gold", "credit_scores", credit)
    return cfg


# --------------------------------------------------------------------------- #
# Comparison                                                                   #
# --------------------------------------------------------------------------- #
RANK_FIELDS = {"p10": 0.10, "p25": 0.25, "median": 0.50, "p75": 0.75, "p90": 0.90}


def _close(a, b) -> bool:
    # Equal to the last displayed digit: sums and means in Trino add in another
    # order than pandas, so a value exactly on a rounding boundary (e.g. a sum of
    # 5-decimal amounts shown to 4) can round either way.
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1.01e-4)


def _assert_equal(got: Any, want: Any, path: str = "") -> None:
    """Exact equality (floats to the last displayed digit)."""
    if isinstance(want, dict):
        assert set(got) == set(want), f"{path}: keys {set(got) ^ set(want)}"
        for k in want:
            _assert_equal(got[k], want[k], f"{path}.{k}")
    elif isinstance(want, list):
        assert len(got) == len(want), f"{path}: {len(got)} != {len(want)} entries"
        for i, (g, w) in enumerate(zip(got, want)):
            _assert_equal(g, w, f"{path}[{i}]")
    elif isinstance(want, float) or isinstance(got, float):
        assert _close(got, want), f"{path}: {got} != {want}"
    else:
        assert got == want, f"{path}: {got!r} != {want!r}"


def _assert_within_rank(values: np.ndarray, dist: dict, path: str) -> None:
    """Each reported percentile lies between the exact values at ranks q ± 1%."""
    x = np.sort(values)
    for field, q in RANK_FIELDS.items():
        lo = np.quantile(x, max(0.0, q - 0.01), method="lower")
        hi = np.quantile(x, min(1.0, q + 0.01), method="higher")
        assert round(lo, 4) - 1e-9 <= dist[field] <= round(hi, 4) + 1e-9, \
            f"{path}.{field}={dist[field]} outside [{lo}, {hi}]"


@pytest.mark.parametrize("tool,kwargs", [
    ("analyze_subscribers_tool", {}),
    ("analyze_usage_events_tool", {"granularity": "month"}),
    ("analyze_usage_events_tool", {"granularity": "week"}),
    ("analyze_usage_events_tool", {"granularity": "day"}),
    ("analyze_recharges_tool", {"granularity": "quarter"}),
    ("analyze_recharges_tool", {"granularity": "week"}),
    ("analyze_fee_charges_tool", {"breakdown": "month"}),
    ("analyze_fee_charges_tool", {"breakdown": "quarter"}),
])
@pytest.mark.parametrize("tenant", TENANTS)
def test_exact_tools_match_the_reference(lake, tool, kwargs, tenant):
    # SCN-MCP-09.2 (exact fields), SCN-MCP-06.2 (only the tenant's rows)
    got = getattr(tools, tool)(tenant, **kwargs)
    want = getattr(ref, tool)(tenant, **kwargs)
    assert "error" not in got, got
    _assert_equal(got, want, tool)


@pytest.mark.parametrize("tool,table", [("analyze_fraud_score_tool", "fraud_scores"),
                                        ("analyze_credit_score_tool", "credit_scores")])
@pytest.mark.parametrize("tenant", TENANTS)
def test_score_tools_match_the_reference(lake, tool, table, tenant):
    # SCN-MCP-09.2 — exact counts/means/histograms; percentiles within ±1% rank
    got = getattr(tools, tool)(tenant)
    want = getattr(ref, tool)(tenant)
    _assert_equal(got, want, tool)
    from conftest import read_lake
    raw = read_lake(lake, "gold", table)
    raw = raw[raw["tenant_id"] == tenant].copy()
    if "affordability_base" in raw:
        raw["affordability_gap"] = raw["affordability_base"] - raw["recommended_limit"]
    ts = pd.to_datetime(raw["scored_at"], utc=True)
    for w, block in got["by_window"].items():
        sub = raw[ts >= ts.max() - pd.Timedelta(days=int(w[:-1]))]
        for metric, dist in block.items():
            if isinstance(dist, dict) and dist.get("count"):
                _assert_within_rank(sub[metric].to_numpy(dtype=float), dist, f"{w}.{metric}")


@pytest.mark.parametrize("sensitivity,view", [(3.0, "monthly"), (2.0, "weekly")])
@pytest.mark.parametrize("tenant", TENANTS)
def test_recharge_anomalies_match_the_reference(lake, tenant, sensitivity, view):
    # SCN-MCP-09.2 — volume spikes and periods exact; amount-spike flags equal
    # except within ±1% of the threshold (approximate median/MAD)
    got = tools.analyze_recharge_anomalies_tool(tenant, sensitivity=sensitivity, view=view, limit=BIG)
    want = ref.analyze_recharge_anomalies_tool(tenant, sensitivity=sensitivity, view=view, limit=BIG)
    assert (got["view"], got["sensitivity"]) == (want["view"], want["sensitivity"])
    key = lambda r: (r["service_id"], r["day"])                       # noqa: E731
    _assert_equal(sorted(got["volume_spikes"], key=key), sorted(want["volume_spikes"], key=key),
                  "volume_spikes")
    g = {key(r): r for r in got["amount_spikes"]}
    w = {key(r): r for r in want["amount_spikes"]}
    for k in g.keys() ^ w.keys():
        z = (g.get(k) or w.get(k))["robust_z"]
        assert abs(z - sensitivity) <= 0.01 * sensitivity + 0.01, f"flag differs away from threshold: {k} z={z}"
    for k in g.keys() & w.keys():
        assert _close(g[k]["amount"], w[k]["amount"])
        assert abs(g[k]["robust_z"] - w[k]["robust_z"]) <= 0.02 * max(1.0, abs(w[k]["robust_z"]))
    if g.keys() == w.keys():
        _assert_equal(got["subscriber_day_anomalies"], want["subscriber_day_anomalies"], "days")
        _assert_equal(got["periodic_view"], want["periodic_view"], "periodic_view")
    else:
        strip = lambda v: [{**p, "anomalous_subscriber_days": 0} for p in v]  # noqa: E731
        _assert_equal(strip(got["periodic_view"]), strip(want["periodic_view"]), "periodic_view")


def test_default_limit_bounds_the_spike_lists(lake):
    out = tools.analyze_recharge_anomalies_tool("tenant_a", sensitivity=0.0)
    assert len(out["amount_spikes"]) <= 50 and len(out["volume_spikes"]) <= 50
    ids = [r["service_id"] for r in out["amount_spikes"] + out["volume_spikes"]]
    assert ids and all(str(i).startswith("tok_") for i in ids)       # tokenized, never raw


# --------------------------------------------------------------------------- #
# REQ-MCP-09 — pushdown: tenant constraints and bounded results                #
# --------------------------------------------------------------------------- #
class Recorder:
    """Wraps ``sql.query``: records each tool query and the rows it fetched."""

    def __init__(self, real):
        self.real, self.calls = real, []

    def __call__(self, cfg, text, params=()):
        df = self.real(cfg, text, params)
        self.calls.append((text, tuple(params), len(df)))
        return df


def _leaves(obj) -> int:
    if isinstance(obj, dict):
        return sum(_leaves(v) for v in obj.values())
    if isinstance(obj, list):
        return sum(_leaves(v) for v in obj)
    return 1


def _inline(text: str, params: tuple) -> str:
    """The statement with its parameters as literals (test-only, trusted values)."""
    parts = text.split("?")
    assert len(parts) == len(params) + 1
    lit = lambda v: (f"'{v}'" if isinstance(v, str)                         # noqa: E731
                     else f"DOUBLE '{v!r}'" if isinstance(v, float) else str(v))
    return "".join(p + (lit(v) if i < len(params) else "")
                   for i, (p, v) in enumerate(zip(parts, [*params, None])))


ALL_CALLS = [(name, {"limit": 50} if "anomalies" in name else {}) for name in tools.TOOLS]


@pytest.mark.parametrize("tool,kwargs", ALL_CALLS)
def test_rows_fetched_are_bounded_by_the_output(lake, monkeypatch, tool, kwargs):
    # SCN-MCP-09.1 — only aggregates cross the wire, never a table's rows
    rec = Recorder(sql.query)
    monkeypatch.setattr(sql, "query", rec)
    out = getattr(tools, tool)("tenant_a", **kwargs)
    fetched = sum(n for text, _, n in rec.calls if "$partitions" not in text)
    assert 0 < fetched <= _leaves(out), (fetched, _leaves(out))


def test_usage_rows_fetched_are_types_times_periods(lake, monkeypatch):
    # SCN-MCP-09.1, literally: event types × periods, plus totals
    rec = Recorder(sql.query)
    monkeypatch.setattr(sql, "query", rec)
    out = tools.analyze_usage_events_tool("tenant_a", granularity="day")
    fetched = sum(n for text, _, n in rec.calls if "$partitions" not in text)
    assert fetched == len(out["usage_over_time"]) + len(out["counts_by_event_type"]) + 1
    assert fetched < out["total_events"] / 10


@pytest.mark.parametrize("tool,kwargs", ALL_CALLS)
def test_every_scan_is_constrained_to_the_tenant(lake, monkeypatch, tool, kwargs):
    # SCN-MCP-09.3 — Trino's IO plan: every lake table scanned carries the
    # predicate tenant_id = <tenant> (the partition column, so only that tenant's
    # partitions are read)
    import json

    rec = Recorder(sql.query)
    monkeypatch.setattr(sql, "query", rec)
    getattr(tools, tool)("tenant_b", **kwargs)
    conn = sql.connect(lake)
    checked = 0
    for text, params, _ in rec.calls:
        if "$partitions" in text or "WHERE false" in text:
            continue
        cur = conn.cursor()
        # EXPLAIN doesn't take bound parameters: plan the statement with them inlined
        cur.execute(f"EXPLAIN (TYPE IO, FORMAT JSON) {_inline(text, params)}")
        plan = json.loads(cur.fetchall()[0][0])
        for scan in plan["inputTableColumnInfos"]:
            cols = {c["columnName"]: c for c in scan["constraint"]["columnConstraints"]}
            assert "tenant_id" in cols, (scan["table"], text)
            ranges = cols["tenant_id"]["domain"]["ranges"]
            assert [(r["low"]["value"], r["high"]["value"]) for r in ranges] == [("tenant_b", "tenant_b")]
            checked += 1
    conn.close()
    assert checked


# --------------------------------------------------------------------------- #
# REQ-PLT-11 / REQ-MCP-06 — the read-only identity                             #
# --------------------------------------------------------------------------- #
def test_the_analytics_identity_is_read_only(lake):
    # SCN-PLT-11.1/11.2, SCN-MCP-06.5
    ok = sql.query(lake, "SELECT tenant_id, count(*) AS n FROM gold.credit_scores GROUP BY 1")
    assert set(ok["tenant_id"]) == set(TENANTS)
    for stmt in ("INSERT INTO silver.dim_subscriber (tenant_id) VALUES ('x')",
                 "DELETE FROM gold.credit_scores WHERE tenant_id = 'tenant_a'",
                 "CREATE TABLE silver.x (a int)",
                 "SELECT count(*) AS tenant_id FROM bronze.parties"):
        with pytest.raises(sql.LakeReadError, match="access denied"):
            sql.query(lake, stmt)


def test_injection_string_returns_no_data(lake):
    # SCN-MCP-06.6
    for fn in tools.TOOLS.values():
        out = fn("x' OR '1'='1")
        assert "action_required" in out and set(out["available_tenants"]) == set(TENANTS)
