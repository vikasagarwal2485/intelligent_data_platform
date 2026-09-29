"""MCP analytics tools over Trino, synthetic fee charges, and the Silver fee fact.

Covers REQ-SYN-04, REQ-SLV-05, and REQ-MCP-01…09 without a query engine: the
tools' queries go to a fake ``sql.query`` that returns canned, grouped results, so
these tests pin the result shaping, the tenant guard and error contract, parameter
binding and the SQL allow-lists. Parity with the pandas reference on a real lake
is ``tests/test_mcp_trino.py`` (marker ``trino``).
"""
from __future__ import annotations

import re

import pandas as pd
import pytest
from trino.exceptions import TrinoConnectionError, TrinoUserError

from idp.mcp import sql
from idp.mcp import tools as mcp_tools
from idp.synthetic.generator import generate

# --------------------------------------------------------------------------- #
# REQ-SYN-04 — synthetic fee charges + catalog/recharge enrichment            #
# --------------------------------------------------------------------------- #
def test_fee_charges_are_normalized_and_typed():
    ds = generate("tenant_a", subscribers=60, days=40, seed=7)
    fees = ds.fee_charges
    assert not fees.empty
    # Normalized: references a product only; no denormalized name/category/service.
    assert {"tenant_id", "id", "product_id", "amount", "charge_type", "charged_at"} <= set(fees.columns)
    assert not ({"service_id", "agreement_id", "product_name", "category"} & set(fees.columns))
    # referential: every product_id resolves to a generated product
    assert set(fees["product_id"]) <= set(ds.products["id"])
    assert set(fees["charge_type"]) <= {"recurring", "one_time", "overage", "discount"}
    assert (fees.loc[fees["charge_type"] == "discount", "amount"] < 0).all()


def test_products_enriched_and_recharge_types_varied():
    ds = generate("tenant_a", subscribers=80, days=40, seed=7)
    assert {"category", "monthly_fee"} <= set(ds.products.columns)
    assert ds.products["monthly_fee"].gt(0).all()
    assert ds.recharge_events["recharge_type"].nunique() > 1


def test_generation_is_deterministic_for_fees():
    a = generate("tenant_a", subscribers=40, days=40, seed=7).fee_charges
    b = generate("tenant_a", subscribers=40, days=40, seed=7).fee_charges
    assert a.equals(b)


# --------------------------------------------------------------------------- #
# REQ-SLV-05 — fact_fee_charge                                                 #
# --------------------------------------------------------------------------- #
def test_fact_fee_charge_enriched_from_products(silver):
    assert "fact_fee_charge" in silver
    fee = silver["fact_fee_charge"]
    assert not fee.empty
    # product_name/category re-derived by joining products; charge carries no PII
    assert {"product_id", "product_name", "category"} <= set(fee.columns)
    assert "service_id" not in fee.columns
    assert fee["product_name"].notna().all()
    assert fee["category"].notna().all()


def test_build_fact_fee_charge_absent_is_safe():
    from idp.transforms.silver import build_fact_fee_charge
    assert build_fact_fee_charge(None, None) is None      # no fee_charges in Bronze -> no table


def test_registry_has_seven_tools():
    assert len(mcp_tools.TOOLS) == 7
    assert {"analyze_fraud_score_tool", "analyze_credit_score_tool"} <= set(mcp_tools.TOOLS)


def test_server_builds_and_registers_tools():
    pytest.importorskip("mcp")
    from idp.mcp.server import build_server

    server = build_server()
    assert server is not None




# --------------------------------------------------------------------------- #
# A fake query engine: canned grouped results, routed by the SQL they answer   #
# --------------------------------------------------------------------------- #
TENANTS = pd.DataFrame({"tenant_id": ["tenant_a", "tenant_b"]})


class FakeEngine:
    """Stands in for ``sql.query``: returns the first canned frame whose markers
    all occur in the SQL, and records every (sql, params) call."""

    def __init__(self, routes=(), tenants=TENANTS):
        self.routes = [((("$partitions",), tenants))] + list(routes)
        self.calls: list[tuple[str, tuple]] = []

    def __call__(self, cfg, text, params=()):
        self.calls.append((text, tuple(params)))
        for markers, result in self.routes:
            if all(m in text for m in markers):
                if isinstance(result, Exception):
                    raise result
                return result.copy()
        return pd.DataFrame()                        # anything else: not written


@pytest.fixture()
def engine(monkeypatch):
    def install(*routes, tenants=TENANTS):
        fake = FakeEngine(routes, tenants)
        monkeypatch.setattr(sql, "query", fake)
        return fake
    return install


def _user_error(name, message=""):
    return TrinoUserError({"errorName": name, "message": message or name})


# --------------------------------------------------------------------------- #
# REQ-MCP-01…05 — result shaping from grouped results                          #
# --------------------------------------------------------------------------- #
def test_subscribers_tool(engine):
    engine(
        (("business_line",), pd.DataFrame({
            "tenant_id": ["tenant_a", "tenant_a", "tenant_b"],
            "business_line": ["consumer", "unknown", "consumer"],
            "n": [6, 2, 99], "devices": [2, 0, 50]})),
        (("is_roaming",), pd.DataFrame({"tenant_id": ["tenant_a"], "roamers": [3]})),
    )
    out = mcp_tools.analyze_subscribers_tool("tenant_a")
    assert out == {"tenant_id": "tenant_a", "subscriber_count": 8, "subscription_count": 6,
                   "device_count": 2, "business_line_mix": {"consumer": 6, "unknown": 2},
                   "roamers": 3, "non_roamers": 5}   # tenant_b's row never reaches the result


def test_usage_events_tool(engine):
    fake = engine(
        (("GROUPING SETS",), pd.DataFrame({
            "tenant_id": ["tenant_a"] * 3, "event_type": [None, "data", "voice"], "g": [1, 0, 0],
            "n": [5, 3, 2], "revenue": [10.5, 7.25, 3.25], "qty": [900, 600, 300],
            "unit": ["MB", "MB", "sec"]})),
        (("AS period",), pd.DataFrame({
            "tenant_id": ["tenant_a"] * 2, "period": ["2026-01", "2026-02"],
            "event_type": ["data", "data"], "events": [1, 2], "revenue": [2.0, 5.25]})),
    )
    out = mcp_tools.analyze_usage_events_tool("tenant_a", granularity="month")
    assert out["total_events"] == 5 == sum(out["counts_by_event_type"].values())
    assert out["revenue_by_event_type"] == {"data": 7.25, "voice": 3.25}
    assert out["duration_by_event_type"]["voice"] == {"total_qty": 300, "unit": "sec"}
    assert out["usage_over_time"][1] == {"period": "2026-02", "event_type": "data",
                                         "events": 2, "revenue": 5.25}
    assert "date_format(started_at, '%Y-%m')" in fake.calls[-1][0]


@pytest.mark.parametrize("granularity,expected", [
    ("day", "'%Y-%m-%d'"), ("week", "week_of_year(started_at)"), ("month", "'%Y-%m'"),
    ("quarter", "quarter(started_at)"), ("month'); DROP TABLE x; --", "'%Y-%m'"),
])
def test_granularity_is_an_allow_list(engine, granularity, expected):
    fake = engine((("GROUPING SETS",), pd.DataFrame({
        "tenant_id": ["tenant_a"], "event_type": [None], "g": [1], "n": [1], "revenue": [1.0],
        "qty": [1], "unit": [None]})))
    mcp_tools.analyze_usage_events_tool("tenant_a", granularity=granularity)
    text = fake.calls[-1][0]
    assert expected in text and "DROP" not in text


def test_recharges_tool(engine):
    engine(
        (("GROUPING SETS",), pd.DataFrame({
            "tenant_id": ["tenant_a"] * 5,
            "recharge_type": [None, "voucher", None, None, None],
            "payment_method": [None, None, "card", None, "unknown"],
            "g": [3, 1, 2, 2, 2], "n": [10, 10, 6, 3, 1], "ok_n": [8, 8, 6, 2, 0],
            "ok_amount": [80.0, 80.0, 60.0, 20.0, 0.0]})),
        (("AS period",), pd.DataFrame({"tenant_id": ["tenant_a"], "period": ["2026-01"],
                                       "amount": [80.0], "n": [8]})),
    )
    out = mcp_tools.analyze_recharges_tool("tenant_a")
    assert (out["total_topup"], out["recharge_count"], out["successful_count"],
            out["average_topup"]) == (80.0, 10, 8, 10.0)
    assert out["by_recharge_type"] == {"count": {"voucher": 10}, "amount": {"voucher": 80.0}}
    # null keys count as "unknown" (merged with a literal "unknown"); amounts only
    # for real keys with successful recharges, like the pandas groupby
    assert out["by_payment_method"] == {"count": {"card": 6, "unknown": 4}, "amount": {"card": 60.0}}
    assert out["by_time"] == [{"period": "2026-01", "amount": 80.0, "count": 8}]


def test_recharge_anomalies_tool(engine):
    fake = engine(
        (("robust_z",), pd.DataFrame({"tenant_id": ["tenant_a"], "service_id": ["tok_1"],
                                      "day": ["2026-01-05"], "amount": [500.0],
                                      "robust_z": [9.123]})),
        (("stddev_pop(recharge_count)",), pd.DataFrame({
            "tenant_id": ["tenant_a"], "service_id": ["tok_2"], "day": ["2026-02-01"],
            "recharge_count": [7], "total_amount": [70.0]})),
        (("AS period",), pd.DataFrame({"tenant_id": ["tenant_a"] * 2,
                                       "period": ["2026-01", "2026-02"],
                                       "total_amount": [900.0, 300.0], "recharge_count": [30, 20]})),
        (("count(*) AS n",), pd.DataFrame({"tenant_id": ["tenant_a"], "n": [50]})),
    )
    out = mcp_tools.analyze_recharge_anomalies_tool("tenant_a", sensitivity=2.5, view="monthly",
                                                    limit=10)
    assert out["amount_spikes"] == [{"service_id": "tok_1", "day": "2026-01-05", "amount": 500.0,
                                     "robust_z": 9.12}]
    assert out["volume_spikes"][0]["recharge_count"] == 7
    assert out["subscriber_day_anomalies"] == [{"service_id": "tok_1", "day": "2026-01-05"},
                                               {"service_id": "tok_2", "day": "2026-02-01"}]
    assert [p["anomalous_subscriber_days"] for p in out["periodic_view"]] == [1, 1]
    # sensitivity and limit are bound parameters, never SQL text
    spikes = next(c for c in fake.calls if "robust_z" in c[0])
    assert spikes[1] == ("tenant_a", 2.5, 10) and "2.5" not in spikes[0]


def test_recharge_anomalies_weekly_view_and_no_recharges(engine):
    fake = engine((("count(*) AS n",), pd.DataFrame({"tenant_id": ["tenant_a"], "n": [3]})))
    out = mcp_tools.analyze_recharge_anomalies_tool("tenant_a", view="weekly")
    assert out["view"] == "week" and "week_of_year(created_at)" in fake.calls[-1][0]
    assert mcp_tools._day_period("2026-01-01", "week") == "2026-W01"
    assert mcp_tools._day_period("2027-01-01", "week") == "2026-W53"   # ISO year, like %G-W%V
    engine()                                          # fact_recharge not written
    assert mcp_tools.analyze_recharge_anomalies_tool("tenant_a")["note"] == "no recharges"


def test_fee_charges_tool(engine):
    engine(
        (("GROUPING SETS",), pd.DataFrame({
            "tenant_id": ["tenant_a"] * 6,
            "product_name": [None, "Unlimited", "Roam+", None, None, None],
            "charge_type": [None, None, None, "recurring", "discount", None],
            "category": [None, None, None, None, None, "plan"],
            "g": [7, 3, 3, 5, 5, 6], "revenue": [90.0, 60.0, 30.0, 100.0, -10.0, 90.0],
            "n": [9, 6, 3, 8, 1, 9]})),
        (("AS period",), pd.DataFrame({"tenant_id": ["tenant_a"], "period": ["2026-Q1"],
                                       "category": ["plan"], "revenue": [90.0], "n": [9]})),
    )
    out = mcp_tools.analyze_fee_charges_tool("tenant_a", breakdown="quarter")
    assert out["total_fee_revenue"] == 90.0 and out["breakdown"] == "quarter"
    assert out["revenue_share_by_product"] == {"Unlimited": 0.6667, "Roam+": 0.3333}
    assert out["by_charge_type"] == {"revenue": {"recurring": 100.0, "discount": -10.0},
                                     "count": {"recurring": 8, "discount": 1}}
    assert out["by_category"]["count"] == {"plan": 9}
    assert out["period_breakdown"] == [{"period": "2026-Q1", "category": "plan",
                                        "revenue": 90.0, "count": 9}]


def test_empty_tenant_tables_are_notes_not_errors(engine):
    engine()                                          # every fact table: not written
    assert mcp_tools.analyze_usage_events_tool("tenant_a")["total_events"] == 0
    assert mcp_tools.analyze_recharges_tool("tenant_a")["recharge_count"] == 0
    assert mcp_tools.analyze_fee_charges_tool("tenant_a")["total_fee_revenue"] == 0.0
    assert mcp_tools.analyze_subscribers_tool("tenant_a")["subscriber_count"] == 0


# --------------------------------------------------------------------------- #
# REQ-MCP-07/08 — score distributions from per-window aggregates               #
# --------------------------------------------------------------------------- #
FRAUD_COLS = ["tenant_id", "service_id", "fraud_proba", "rule_score", "fraud_score", "scored_at"]


def _stats_row(w, n, metrics, lo=0.0, hi=1.0):
    """The long-format aggregate rows of one window: its row count (`_rows`) and,
    per metric, the stats and the order statistics around each quantile."""
    rows = [{"tenant_id": "tenant_a", "w": w, "metric": "_rows", "n": n}]
    for m in metrics:
        row = {"tenant_id": "tenant_a", "w": w, "metric": m, "n": n, "mean": 0.5, "std": 0.25,
               "lo": lo, "hi": hi}
        for k, v in enumerate((0.1, 0.2, 0.5, 0.8, 0.9)):
            row.update({f"a{k}": v, f"b{k}": v})
        rows.append(row)
    return rows


def _score_engine(engine, cols, metrics, n_ts=8, stats=None, hist=None):
    return engine(
        (("AS bin",), hist if hist is not None else pd.DataFrame(
            {"tenant_id": ["tenant_a"] * 2, "w": [30, 30], "metric": [metrics[0]] * 2,
             "bin": [0, 7], "n": [5, 3]})),
        (("FROM o GROUP BY",), pd.DataFrame(
            stats or [*_stats_row(7, 3, metrics), *_stats_row(30, 8, metrics)])),
        (("WHERE false",), pd.DataFrame(columns=cols)),
        (("FROM gold.",), pd.DataFrame({"tenant_id": ["tenant_a"], "n": [8], "n_ts": [n_ts]})),
    )


def test_fraud_score_tool_windows(engine):
    metrics = ["fraud_proba", "rule_score", "fraud_score"]
    _score_engine(engine, FRAUD_COLS, metrics)
    out = mcp_tools.analyze_fraud_score_tool("tenant_a")
    assert out["windows_days"] == [7, 15, 30]
    bw = out["by_window"]
    assert set(bw) == {"7d", "15d", "30d"}
    assert (bw["7d"]["count"], bw["15d"]["count"], bw["30d"]["count"]) == (3, 0, 8)
    assert bw["15d"]["fraud_proba"] == {"count": 0}          # an empty window
    dist = bw["30d"]["fraud_proba"]
    assert set(dist) == {"count", "mean", "std", "min", "p10", "p25", "median", "p75", "p90",
                         "max", "histogram"}
    assert dist["median"] == 0.5 and len(dist["histogram"]) == 8
    assert dist["histogram"][0] == {"lo": 0.0, "hi": 0.125, "count": 5}
    assert dist["histogram"][7] == {"lo": 0.875, "hi": 1.0, "count": 3}
    assert "note" not in bw["7d"]


def test_percentiles_interpolate_like_pandas():
    # the two order statistics around each quantile, interpolated as numpy's
    # `linear` method (pandas' default) does
    import numpy as np
    rng = np.random.default_rng(3)
    for n in (1, 2, 5, 8, 101):
        x = np.sort(rng.gamma(2, 3, n))
        for q in mcp_tools._QUANTILES:
            f = int(np.floor((n - 1) * q))
            got = mcp_tools._lerp_quantile(n, q, x[f], x[min(f + 1, n - 1)])
            assert got == pd.Series(x).quantile(q)


def test_credit_score_tool_gap_and_zero_width_histogram(engine):
    metrics = ["pd", "affordability_base", "recommended_limit", "affordability_gap"]
    cols = ["tenant_id", "service_id", "pd", "affordability_base", "recommended_limit", "scored_at"]
    fake = _score_engine(engine, cols, metrics,
                         stats=[r for w in (7, 15, 30) for r in _stats_row(w, 4, metrics, lo=2.0, hi=2.0)])
    out = mcp_tools.analyze_credit_score_tool("tenant_a")
    bw = out["by_window"]
    for w in ("7d", "15d", "30d"):
        assert set(metrics) <= set(bw[w])
    hist = bw["30d"]["affordability_gap"]["histogram"]
    assert (hist[0]["lo"], hist[-1]["hi"]) == (1.5, 2.5)     # np.histogram widens by 0.5
    stats_sql = next(c[0] for c in fake.calls if "FROM o GROUP BY" in c[0])
    assert "affordability_base - recommended_limit" in stats_sql


def test_score_snapshot_note_without_timestamp(engine):
    metrics = ["fraud_proba", "rule_score", "fraud_score"]
    fake = _score_engine(engine, FRAUD_COLS[:-1], metrics,
                         stats=[r for w in (7, 15, 30) for r in _stats_row(w, 2, metrics)])
    out = mcp_tools.analyze_fraud_score_tool("tenant_a")
    assert all(out["by_window"][w]["count"] == 2 for w in ("7d", "15d", "30d"))
    assert "note" in out["by_window"]["7d"]
    assert not any("scored_at" in c[0] for c in fake.calls)


def test_score_tools_without_scores(engine):
    engine()                                          # gold tables not written
    out = mcp_tools.analyze_fraud_score_tool("tenant_b")
    assert out["tenant_id"] == "tenant_b" and out["count"] == 0 and "note" in out


# --------------------------------------------------------------------------- #
# REQ-MCP-06 — tenant guard, error contract, bound parameters                  #
# --------------------------------------------------------------------------- #
def test_missing_tenant_asks_with_available_list(engine):
    # SCN-MCP-06.3
    fake = engine()
    for fn in mcp_tools.TOOLS.values():
        for tenant in ("", "tenant_zzz"):
            out = fn(tenant)
            assert out["available_tenants"] == ["tenant_a", "tenant_b"]
            assert "action_required" in out and "error" in out
    assert all("$partitions" in c[0] for c in fake.calls)    # no tool query ran


def test_injection_string_is_an_unknown_tenant(engine):
    # SCN-MCP-06.6
    fake = engine()
    evil = "x' OR '1'='1"
    for fn in mcp_tools.TOOLS.values():
        out = fn(evil)
        assert out["tenant_id"] == evil and "action_required" in out
    assert not any(evil in c[0] or evil in c[1] for c in fake.calls)


def test_tenant_is_always_a_bound_parameter(engine):
    fake = engine(
        (("GROUPING SETS",), pd.DataFrame({"tenant_id": ["tenant_a"], "event_type": [None],
                                           "g": [1], "n": [1], "revenue": [1.0], "qty": [1],
                                           "unit": [None]})))
    for fn in mcp_tools.TOOLS.values():
        fn("tenant_a")
    tool_calls = [c for c in fake.calls if "$partitions" not in c[0]]
    assert tool_calls
    for text, params in tool_calls:
        assert "tenant_a" not in text
        if "WHERE false" not in text:
            assert text.count("?") == len(params) and "tenant_a" in params


def test_unreachable_engine_surfaces_error_not_empty(monkeypatch):
    # SCN-MCP-06.4 — through the real query layer, with the connection refused
    def refused(cfg):
        raise TrinoConnectionError("Connection refused")
    monkeypatch.setattr(sql, "connect", refused)
    for fn in mcp_tools.TOOLS.values():
        out = fn("tenant_a")
        assert "Connection refused" in out["error"]
        assert out["source"] == "http://127.0.0.1:8085"
        assert not ({"subscriber_count", "total_events", "by_window"} & set(out))


def test_access_denied_is_an_error(engine):
    engine((("business_line",), sql.LakeReadError("access denied by the query engine: x",
                                                  "http://127.0.0.1:8085")))
    out = mcp_tools.analyze_subscribers_tool("tenant_a")
    assert out["error"].startswith("access denied") and out["source"] == "http://127.0.0.1:8085"


def test_absent_lake_reports_no_tenants(engine):
    engine(tenants=pd.DataFrame())                    # dim_subscriber not written
    out = mcp_tools.analyze_subscribers_tool("tenant_a")
    assert out["available_tenants"] == [] and "error" in out and out["source"]


@pytest.mark.parametrize("route,tool,table", [
    ((("GROUPING SETS",), _user_error("COLUMN_NOT_FOUND", "line 1:8: Column 'tenant_id' cannot be resolved")),
     "analyze_usage_events_tool", "fact_usage"),
    ((("$partitions",), _user_error("COLUMN_NOT_FOUND", "line 1:17: Column 'partition' cannot be resolved")),
     "analyze_subscribers_tool", "dim_subscriber"),
])
def test_untagged_table_is_an_isolation_error(engine, route, tool, table):
    # SCN-FND-06.1
    fake = engine()
    fake.routes.insert(0, route)
    out = getattr(mcp_tools, tool)("tenant_a")
    assert out["error"].startswith("tenant isolation violation") and out["table"] == table
    assert not ({"total_events", "subscriber_count", "business_line_mix"} & set(out))


def test_tools_read_through_the_tenant_gateway(engine, monkeypatch):
    # SCN-FND-06.2 — every tool's rows come from scope_to_tenant for the caller's tenant
    metrics = ["fraud_proba", "rule_score", "fraud_score"]
    fake = _score_engine(engine, FRAUD_COLS, metrics)
    fake.routes[1:1] = [(("GROUPING SETS",), pd.DataFrame({
        "tenant_id": ["tenant_a"], "event_type": [None], "g": [1], "n": [1], "revenue": [1.0],
        "qty": [1], "unit": [None]})),
        (("business_line",), pd.DataFrame({"tenant_id": ["tenant_a"], "business_line": ["x"],
                                           "n": [1], "devices": [0]})),
        (("fact_recharge", "count(*) AS n"), pd.DataFrame({"tenant_id": ["tenant_a"], "n": [1]}))]
    calls = []
    real = mcp_tools.scope_to_tenant
    monkeypatch.setattr(mcp_tools, "scope_to_tenant", lambda df, t: calls.append(t) or real(df, t))
    for name, fn in mcp_tools.TOOLS.items():
        calls.clear()
        fn("tenant_a")
        assert calls and set(calls) == {"tenant_a"}, name


def test_tool_sql_names_only_code_constants():
    # Every table the tools name is a Silver/Gold constant (never Bronze, never input).
    import inspect
    src = inspect.getsource(mcp_tools)
    tables = set(re.findall(r"\b(bronze|silver|gold)\.\{?(\w+)", src))
    assert {layer for layer, _ in tables} <= {"silver", "gold"}
