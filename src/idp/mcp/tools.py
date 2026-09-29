"""Read-only, tenant-scoped analytics over the lake, answered by Trino (REQ-MCP-01…09).

Seven functions, one per business-question set, that return compact, JSON-able
aggregates for one tenant. Each asks Trino's ``lake`` catalog through
``mcp.sql`` as the read-only ``idp-mcp`` identity. The filtering, grouping, sums,
counts, histograms and rank statistics run in Trino over the tenant's partitions;
Python only shapes the small grouped results into the output contract. The MCP
server (``server.py``) registers these as tools; they are equally callable in tests.

* **Tenant.** Every query filters on the tenant as a bound parameter, selects
  ``tenant_id`` among its outputs, and its result passes through the tenant gateway
  (``_scoped``) before use (REQ-FND-06).
* **SQL text** is built only from code constants (tables, columns, the period
  allow-list). Values the caller controls (``tenant_id``, ``sensitivity``,
  ``limit``) are always bound.
* **Parity.** ``tests/mcp_reference.py`` keeps the exact pandas implementations
  this replaced, and the tools reproduce them: counts, sums, means, histograms,
  and exact medians and percentiles (window-function order statistics, not
  ``approx_percentile``, whose T-Digest misses REQ-MCP-09's ±1% rank bound on
  small or discrete data; ADR-0030).
"""
from __future__ import annotations

import functools
import math
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from trino.exceptions import TrinoUserError

from idp.common.config import PlatformConfig, load_config
from idp.common.tenancy import TenantIsolationError, require_tenant_column, scope_to_tenant
from idp.mcp import sql
from idp.mcp.sql import LakeReadError  # noqa: F401 - the tools' error contract

# Silver tables the tools read.
_DIM = "dim_subscriber"
_USAGE = "fact_usage"
_RECHARGE = "fact_recharge"
_FEES = "fact_fee_charge"
# Gold score tables (produced by `idp score`).
_CREDIT_SCORES = "credit_scores"
_FRAUD_SCORES = "fraud_scores"
_SCORE_WINDOWS = (7, 15, 30)  # days
_QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)
_BINS = 8

# Period labels, identical to the pandas `_period` formats. The granularity is
# mapped through this allow-list, never formatted into SQL from input.
_PERIOD_SQL = {
    "day": "date_format({c}, '%Y-%m-%d')",
    "week": ("concat(CAST(year_of_week({c}) AS varchar), '-W', "
             "lpad(CAST(week_of_year({c}) AS varchar), 2, '0'))"),
    "month": "date_format({c}, '%Y-%m')",
    "quarter": "concat(CAST(year({c}) AS varchar), '-Q', CAST(quarter({c}) AS varchar))",
}


def _cfg(config_path: str | None) -> PlatformConfig:
    return load_config(config_path)


def _period_sql(column: str, granularity: str) -> str:
    g = (granularity or "month").lower()
    return _PERIOD_SQL.get(g, _PERIOD_SQL["month"]).format(c=column)


def _day_period(day: str, granularity: str) -> str:
    """The week/month label of a ``YYYY-MM-DD`` day (the periodic anomaly view)."""
    d = date.fromisoformat(day)
    if granularity == "week":
        y, w, _ = d.isocalendar()
        return f"{y}-W{w:02d}"
    return d.strftime("%Y-%m")


def _isolation_error(table: str, err: TenantIsolationError) -> TenantIsolationError:
    """Name the offending table on a tenant-isolation violation."""
    wrapped = TenantIsolationError(f"table {table!r}: {err}")
    wrapped.table = table
    return wrapped


def _scoped(df: pd.DataFrame, tenant_id: str, table: str) -> pd.DataFrame:
    """The tenant's rows via the single tenant gateway (REQ-FND-02/06).

    A result without a ``tenant_id`` column is an isolation violation, never an
    empty or unscoped result.
    """
    try:
        require_tenant_column(df)
    except TenantIsolationError as e:
        raise _isolation_error(table, e) from e
    df["tenant_id"] = df["tenant_id"].astype(str)
    return scope_to_tenant(df, tenant_id)


def _untagged(e: TrinoUserError, *names: str) -> bool:
    """Trino couldn't resolve the tenant column: the table isn't tenant-stamped."""
    return e.error_name == "COLUMN_NOT_FOUND" and any(n in (e.message or "") for n in names)


def _q(cfg: PlatformConfig, tenant_id: str, table: str, text: str,
       params: tuple = ()) -> pd.DataFrame:
    """Run one tool query for ``tenant_id``; its result, through the gateway.

    ``table`` names the table a tenant-isolation violation is reported against.
    An empty frame without columns means the table was never written.
    """
    try:
        df = sql.query(cfg, text, params)
    except TrinoUserError as e:
        if _untagged(e, "tenant_id"):
            raise _isolation_error(table, TenantIsolationError(
                "table has no 'tenant_id' column; every record must be tenant-stamped")) from e
        raise
    if len(df.columns) == 0:
        return df
    return _scoped(df, str(tenant_id), table)


def _available_tenants(cfg: PlatformConfig) -> list[str]:
    """Tenants present in Silver, from ``dim_subscriber``'s partition metadata."""
    try:
        return sql.available_tenants(cfg)
    except TrinoUserError as e:
        if _untagged(e, "tenant_id", "partition"):
            raise _isolation_error(_DIM, TenantIsolationError(
                "table is not partitioned by 'tenant_id'; every record must be "
                "tenant-stamped")) from e
        raise


def _lake_guard(fn):
    """Validate the tenant and turn lake failures into structured results (never raise).

    Keeps the read-only contract and makes the ambiguous cases explicit:

    * **Unreachable or refusing engine** -> ``{"error", "source"}`` (not a
      misleading empty ``0``); ``source`` is the Trino endpoint.
    * **Untagged table** (no ``tenant_id``) -> ``{"error": "tenant isolation
      violation: ...", "table"}``; never served as empty or unscoped data.
    * **Missing/unknown tenant_id** -> the list of tenants that actually exist plus
      an ``action_required`` telling the client to ASK the user which tenant rather
      than guess. The tenant is only ever compared with that list, so a value with
      SQL metacharacters is just an unknown tenant.
    """
    @functools.wraps(fn)
    def wrapper(tenant_id: str = "", **kwargs):
        cfg = _cfg(kwargs.get("config_path"))
        try:
            tenants = _available_tenants(cfg)
            if not tenants:
                return {"tenant_id": tenant_id or None,
                        "error": "no tenant data found in the lake (nothing ingested "
                                 "yet, or the query engine points at another lake)",
                        "source": sql.endpoint(cfg),
                        "available_tenants": []}
            if not tenant_id or str(tenant_id) not in tenants:
                return {"tenant_id": tenant_id or None,
                        "error": f"tenant_id {tenant_id!r} is missing or unknown",
                        "available_tenants": tenants,
                        "action_required": "Ask the user which tenant to analyze; do "
                                           "not guess. Then call again with that tenant_id."}
            return fn(tenant_id, **kwargs)
        except LakeReadError as e:
            return {"tenant_id": tenant_id or None, "error": str(e),
                    "source": e.source or sql.endpoint(cfg)}
        except TenantIsolationError as e:
            # Untagged data must never be served as empty or unscoped (REQ-FND-06).
            return {"tenant_id": tenant_id or None,
                    "error": f"tenant isolation violation: {e}",
                    "table": getattr(e, "table", None)}
    return wrapper


def _num(v: Any, default: float = 0.0) -> float:
    return default if v is None or pd.isna(v) else float(v)


def _round_map(d: dict, ndigits: int = 2) -> dict:
    return {k: round(float(v), ndigits) for k, v in d.items()}


def _grouping_sets(df: pd.DataFrame, mask: int, key: str) -> pd.DataFrame:
    """The rows of one grouping set (``grouping(...)`` bitmask ``mask``: a bit is set
    for each argument the set doesn't group by), with null ``key`` values dropped as
    pandas ``groupby`` drops them."""
    part = df[df["g"] == mask]
    return part[part[key].notna()]


# --------------------------------------------------------------------------- #
# Tool 1 — subscribers                                                         #
# --------------------------------------------------------------------------- #
@_lake_guard
def analyze_subscribers_tool(tenant_id: str, *, config_path: str | None = None) -> dict[str, Any]:
    """Subscriber base analytics for one tenant (REQ-MCP-01):
    subscription vs device counts, business-line mix, roamer vs non-roamer split.
    """
    cfg = _cfg(config_path)
    mix = _q(cfg, tenant_id, _DIM,
             "SELECT tenant_id, coalesce(business_line, 'unknown') AS business_line, "
             "count(*) AS n, count_if(coalesce(has_device_agreement, false)) AS devices "
             f"FROM silver.{_DIM} WHERE tenant_id = ? GROUP BY 1, 2", (tenant_id,))
    if mix.empty:
        return {"tenant_id": tenant_id, "subscriber_count": 0, "note": "no subscribers for tenant"}

    subscriber_count = int(mix["n"].sum())
    device_count = int(mix["devices"].sum())
    # Roamer = subscriber with >=1 roaming usage event.
    roam = _q(cfg, tenant_id, _USAGE,
              "SELECT d.tenant_id, count(*) AS roamers "
              f"FROM silver.{_DIM} d WHERE d.tenant_id = ? AND d.service_id IN "
              f"(SELECT u.service_id FROM silver.{_USAGE} u WHERE u.tenant_id = ? AND u.is_roaming) "
              "GROUP BY 1", (tenant_id, tenant_id))
    roamers = int(roam["roamers"].sum()) if not roam.empty else 0

    return {
        "tenant_id": tenant_id,
        "subscriber_count": subscriber_count,
        "subscription_count": subscriber_count - device_count,
        "device_count": device_count,
        "business_line_mix": {str(k): int(v) for k, v in zip(mix["business_line"], mix["n"])},
        "roamers": roamers,
        "non_roamers": subscriber_count - roamers,
    }


# --------------------------------------------------------------------------- #
# Tool 2 — usage events                                                        #
# --------------------------------------------------------------------------- #
@_lake_guard
def analyze_usage_events_tool(
    tenant_id: str, *, granularity: str = "month", config_path: str | None = None
) -> dict[str, Any]:
    """Usage-event analytics for one tenant (REQ-MCP-02):
    total events, revenue per event type, duration/volume per event type, and a
    time series of events + revenue by event type.
    """
    cfg = _cfg(config_path)
    agg = _q(cfg, tenant_id, _USAGE,
             "SELECT tenant_id, event_type, grouping(event_type) AS g, count(*) AS n, "
             "coalesce(sum(gross_amount), 0e0) AS revenue, coalesce(sum(qty), 0) AS qty, "
             f"min(unit) AS unit FROM silver.{_USAGE} WHERE tenant_id = ? "
             "GROUP BY GROUPING SETS ((tenant_id), (tenant_id, event_type))", (tenant_id,))
    total = agg[agg["g"] == 1] if not agg.empty else agg
    if total.empty or int(total["n"].iloc[0]) == 0:
        return {"tenant_id": tenant_id, "total_events": 0, "note": "no usage events for tenant"}
    by_type = _grouping_sets(agg, 0, "event_type")

    ts = _q(cfg, tenant_id, _USAGE,
            f"SELECT tenant_id, {_period_sql('started_at', granularity)} AS period, event_type, "
            "count(*) AS events, coalesce(sum(gross_amount), 0e0) AS revenue "
            f"FROM silver.{_USAGE} WHERE tenant_id = ? AND started_at IS NOT NULL "
            "AND event_type IS NOT NULL GROUP BY 1, 2, 3 ORDER BY 2, 3", (tenant_id,))

    return {
        "tenant_id": tenant_id,
        "granularity": granularity,
        "total_events": int(total["n"].iloc[0]),
        "counts_by_event_type": {str(r.event_type): int(r.n) for r in by_type.itertuples()},
        "revenue_by_event_type": {str(r.event_type): round(_num(r.revenue), 4)
                                  for r in by_type.itertuples()},
        "duration_by_event_type": {
            str(r.event_type): {"total_qty": int(_num(r.qty)),
                                "unit": None if r.unit is None or pd.isna(r.unit) else r.unit}
            for r in by_type.itertuples()},
        "usage_over_time": [
            {"period": r.period, "event_type": r.event_type, "events": int(r.events),
             "revenue": round(_num(r.revenue), 4)}
            for r in ts.itertuples()
        ],
    }


# --------------------------------------------------------------------------- #
# Tool 3 — recharges                                                           #
# --------------------------------------------------------------------------- #
@_lake_guard
def analyze_recharges_tool(
    tenant_id: str, *, granularity: str = "month", config_path: str | None = None
) -> dict[str, Any]:
    """Recharge analytics for one tenant (REQ-MCP-03):
    total top-up, count, average value, and splits by recharge_type,
    payment_method and time.
    """
    cfg = _cfg(config_path)
    agg = _q(cfg, tenant_id, _RECHARGE,
             "SELECT tenant_id, recharge_type, payment_method, "
             "grouping(recharge_type, payment_method) AS g, count(*) AS n, "
             "count_if(status = 'successful') AS ok_n, "
             "coalesce(sum(amount) FILTER (WHERE status = 'successful'), 0e0) AS ok_amount "
             f"FROM silver.{_RECHARGE} WHERE tenant_id = ? GROUP BY GROUPING SETS "
             "((tenant_id), (tenant_id, recharge_type), (tenant_id, payment_method))",
             (tenant_id,))
    total = agg[agg["g"] == 3] if not agg.empty else agg
    if total.empty or int(total["n"].iloc[0]) == 0:
        return {"tenant_id": tenant_id, "recharge_count": 0, "note": "no recharges for tenant"}
    t = total.iloc[0]
    total_topup = _num(t["ok_amount"])
    successful_count = int(t["ok_n"])

    def _split(col: str, mask: int) -> dict[str, Any]:
        part = agg[agg["g"] == mask]
        count: dict[str, int] = {}
        for k, n in zip(part[col], part["n"]):
            key = "unknown" if k is None or pd.isna(k) else str(k)   # fillna("unknown")
            count[key] = count.get(key, 0) + int(n)
        ok = part[part[col].notna() & (part["ok_n"] > 0)]
        return {"count": count,
                "amount": {str(k): round(_num(v), 2) for k, v in zip(ok[col], ok["ok_amount"])}}

    by_time = _q(cfg, tenant_id, _RECHARGE,
                 f"SELECT tenant_id, {_period_sql('created_at', granularity)} AS period, "
                 "coalesce(sum(amount), 0e0) AS amount, count(*) AS n "
                 f"FROM silver.{_RECHARGE} WHERE tenant_id = ? AND status = 'successful' "
                 "AND created_at IS NOT NULL GROUP BY 1, 2 ORDER BY 2", (tenant_id,))

    return {
        "tenant_id": tenant_id,
        "granularity": granularity,
        "total_topup": round(total_topup, 2),
        "recharge_count": int(t["n"]),
        "successful_count": successful_count,
        "average_topup": round(total_topup / successful_count, 2) if successful_count else 0.0,
        "by_recharge_type": _split("recharge_type", 1),     # grouping bit set: payment_method
        "by_payment_method": _split("payment_method", 2),
        "by_time": [{"period": r.period, "amount": round(_num(r.amount), 2), "count": int(r.n)}
                    for r in by_time.itertuples()],
    }


# --------------------------------------------------------------------------- #
# Tool 4 — recharge anomalies                                                  #
# --------------------------------------------------------------------------- #
# The tenant's successful recharges, with the UTC day.
_OK_RECHARGES = (
    "ok AS (SELECT tenant_id, service_id, CAST(amount AS double) AS amount, created_at, "
    "date_format(created_at, '%Y-%m-%d') AS day "
    f"FROM silver.{_RECHARGE} WHERE tenant_id = ? AND status = 'successful')"
)


def _median_sql(col: str, source: str, alias: str) -> str:
    """An exact median as pandas computes it: the mean of the middle one or two
    values (the numbered rows at ``(n-1)/2`` and ``n/2``)."""
    return (f"SELECT avg({col}) AS {alias} FROM (SELECT {col}, "
            f"row_number() OVER (ORDER BY {col}) - 1 AS i, count(*) OVER () AS n FROM {source}) "
            "WHERE i IN ((n - 1) / 2, n / 2)")


@_lake_guard
def analyze_recharge_anomalies_tool(
    tenant_id: str,
    *,
    sensitivity: float = 3.0,
    view: str = "monthly",
    limit: int = 50,
    config_path: str | None = None,
) -> dict[str, Any]:
    """Recharge-anomaly analytics for one tenant (REQ-MCP-04):
    amount spikes, volume spikes, subscriber-day anomalies, and weekly/monthly
    views. ``sensitivity`` is the robust z threshold (median/MAD).
    """
    cfg = _cfg(config_path)
    k, limit = float(sensitivity), max(0, int(limit))
    n = _q(cfg, tenant_id, _RECHARGE,
           f"SELECT tenant_id, count(*) AS n FROM silver.{_RECHARGE} WHERE tenant_id = ? GROUP BY 1",
           (tenant_id,))
    if n.empty:
        return {"tenant_id": tenant_id, "amount_spikes": [], "volume_spikes": [], "note": "no recharges"}

    # (a) amount spikes — robust z-score = 0.6745*(x - median)/MAD
    spikes = _q(cfg, tenant_id, _RECHARGE,
                f"WITH {_OK_RECHARGES}, "
                f"m AS ({_median_sql('amount', 'ok WHERE amount IS NOT NULL', 'med')}), "
                "dv AS (SELECT abs(ok.amount - m.med) AS dev FROM ok CROSS JOIN m "
                "WHERE ok.amount IS NOT NULL), "
                f"d AS ({_median_sql('dev', 'dv', 'mad')}), "
                "z AS (SELECT ok.tenant_id, ok.service_id, ok.day, ok.amount, "
                "0.6745e0 * (ok.amount - m.med) / d.mad AS robust_z "
                "FROM ok CROSS JOIN m CROSS JOIN d WHERE d.mad > 0) "
                "SELECT tenant_id, service_id, day, amount, robust_z FROM z WHERE robust_z >= ? "
                "ORDER BY amount DESC, service_id, day LIMIT ?", (tenant_id, k, limit))
    amount_spikes = [
        {"service_id": r.service_id, "day": r.day, "amount": round(float(r.amount), 2),
         "robust_z": round(float(r.robust_z), 2)}
        for r in spikes.itertuples()
    ]

    # (b)/(c) subscriber-day volume — recharges per (service_id, day) above
    # max(2, mean + k·std) of all the tenant's subscriber-days
    vol = _q(cfg, tenant_id, _RECHARGE,
             f"WITH {_OK_RECHARGES}, "
             "sd AS (SELECT tenant_id, service_id, day, count(*) AS recharge_count, "
             "coalesce(sum(amount), 0e0) AS total_amount FROM ok "
             "WHERE service_id IS NOT NULL AND day IS NOT NULL GROUP BY 1, 2, 3), "
             "t AS (SELECT greatest(2e0, avg(recharge_count) + ? * stddev_pop(recharge_count)) AS thr "
             "FROM sd) "
             "SELECT sd.* FROM sd CROSS JOIN t WHERE sd.recharge_count >= t.thr "
             "ORDER BY recharge_count DESC, service_id, day LIMIT ?", (tenant_id, k, limit))
    volume_spikes = [
        {"service_id": r.service_id, "day": r.day, "recharge_count": int(r.recharge_count),
         "total_amount": round(float(r.total_amount), 2)}
        for r in vol.itertuples()
    ]
    anomalous_days = {(r["service_id"], r["day"]) for r in volume_spikes}
    anomalous_days |= {(r["service_id"], r["day"]) for r in amount_spikes}
    subscriber_day_anomalies = [{"service_id": s, "day": d} for s, d in sorted(anomalous_days)][:limit]

    # (d) periodic view
    gran = "week" if str(view).lower().startswith("week") else "month"
    per = _q(cfg, tenant_id, _RECHARGE,
             f"WITH {_OK_RECHARGES} "
             f"SELECT tenant_id, {_period_sql('created_at', gran)} AS period, "
             "coalesce(sum(amount), 0e0) AS total_amount, count(*) AS recharge_count "
             "FROM ok WHERE created_at IS NOT NULL GROUP BY 1, 2 ORDER BY 2", (tenant_id,))
    anom_by_period: dict[str, int] = {}
    for _, d in anomalous_days:
        p = _day_period(d, gran)
        anom_by_period[p] = anom_by_period.get(p, 0) + 1
    periodic_view = [
        {"period": r.period, "total_amount": round(_num(r.total_amount), 2),
         "recharge_count": int(r.recharge_count),
         "anomalous_subscriber_days": int(anom_by_period.get(r.period, 0))}
        for r in per.itertuples()
    ]

    return {
        "tenant_id": tenant_id,
        "sensitivity": sensitivity,
        "view": gran,
        "amount_spikes": amount_spikes,
        "volume_spikes": volume_spikes,
        "subscriber_day_anomalies": subscriber_day_anomalies,
        "periodic_view": periodic_view,
    }


# --------------------------------------------------------------------------- #
# Tool 5 — fee charges                                                         #
# --------------------------------------------------------------------------- #
@_lake_guard
def analyze_fee_charges_tool(
    tenant_id: str, *, breakdown: str = "month", config_path: str | None = None
) -> dict[str, Any]:
    """Product fee-charge analytics for one tenant (REQ-MCP-05):
    product fee revenue, product revenue distribution, charge-type analysis, and
    product/category/month/quarter breakdowns.
    """
    cfg = _cfg(config_path)
    agg = _q(cfg, tenant_id, _FEES,
             "SELECT tenant_id, product_name, charge_type, category, "
             "grouping(product_name, charge_type, category) AS g, "
             "coalesce(sum(amount), 0e0) AS revenue, count(*) AS n "
             f"FROM silver.{_FEES} WHERE tenant_id = ? GROUP BY GROUPING SETS "
             "((tenant_id), (tenant_id, product_name), (tenant_id, charge_type), "
             "(tenant_id, category))", (tenant_id,))
    total = agg[agg["g"] == 7] if not agg.empty else agg
    if total.empty or int(total["n"].iloc[0]) == 0:
        return {"tenant_id": tenant_id, "total_fee_revenue": 0.0, "note": "no fee charges for tenant"}

    products = _grouping_sets(agg, 3, "product_name")
    rev_by_product = {str(k): _num(v) for k, v in zip(products["product_name"], products["revenue"])}
    # revenue share sums to ~1.0 (net of discounts); guard divide-by-zero
    share_base = sum(rev_by_product.values())

    def _by(col: str, mask: int) -> dict[str, Any]:
        part = _grouping_sets(agg, mask, col)
        return {"revenue": {str(k): round(_num(v), 2) for k, v in zip(part[col], part["revenue"])},
                "count": {str(k): int(v) for k, v in zip(part[col], part["n"])}}

    gran = "quarter" if str(breakdown).lower().startswith("q") else "month"
    per = _q(cfg, tenant_id, _FEES,
             f"SELECT tenant_id, {_period_sql('charged_at', gran)} AS period, category, "
             "coalesce(sum(amount), 0e0) AS revenue, count(*) AS n "
             f"FROM silver.{_FEES} WHERE tenant_id = ? AND charged_at IS NOT NULL "
             "AND category IS NOT NULL GROUP BY 1, 2, 3 ORDER BY 2, 3", (tenant_id,))

    return {
        "tenant_id": tenant_id,
        "breakdown": gran,
        "total_fee_revenue": round(_num(total["revenue"].iloc[0]), 2),
        "revenue_by_product": _round_map(rev_by_product, 2),
        "revenue_share_by_product": (
            {k: round(v / share_base, 4) for k, v in rev_by_product.items()} if share_base else {}),
        "by_charge_type": _by("charge_type", 5),
        "by_category": _by("category", 6),
        "period_breakdown": [
            {"period": r.period, "category": r.category, "revenue": round(_num(r.revenue), 2),
             "count": int(r.n)}
            for r in per.itertuples()
        ],
    }


# --------------------------------------------------------------------------- #
# Score distributions (tools 6 and 7)                                          #
# --------------------------------------------------------------------------- #
# Derived metrics: name -> SQL over the score table's columns (and what it needs).
_DERIVED = {"affordability_gap": ("affordability_base - recommended_limit",
                                  {"affordability_base", "recommended_limit"})}


def _lerp_quantile(n: int, q: float, a: float, b: float) -> float:
    """numpy's ``linear`` quantile from the two order statistics around it: ``a`` at
    index ``floor((n-1)·q)`` and ``b`` at the next one (bit-identical to pandas)."""
    vi = (n - 1) * q
    g = vi - math.floor(vi)
    d = b - a
    return b - d * (1 - g) if g >= 0.5 else a + d * g


def _order_stat_sql(q: float, k: int) -> str:
    """Aggregates picking the order statistics for quantile ``q`` (columns
    ``a<k>``/``b<k>``) from rows numbered ``i`` (0-based) of ``cnt`` in their group."""
    f = f"CAST(floor((cnt - 1) * {q}e0) AS bigint)"
    return (f"max(v) FILTER (WHERE i = {f}) AS a{k}, "
            f"max(v) FILTER (WHERE i = least({f} + 1, cnt - 1)) AS b{k}")


def _distribution(stats: pd.Series | None, hist: dict[int, int]) -> dict[str, Any]:
    """The compact distribution of one metric from its aggregate row: count, summary
    stats, exact percentiles, and the 8-bin histogram (``np.histogram``'s bins)."""
    n = 0 if stats is None else int(stats["n"])
    if n == 0:
        return {"count": 0}
    lo, hi = float(stats["lo"]), float(stats["hi"])
    q = [_lerp_quantile(n, qq, float(stats[f"a{k}"]), float(stats[f"b{k}"]))
         for k, qq in enumerate(_QUANTILES)]
    edges = np.linspace(lo - 0.5, hi + 0.5, _BINS + 1) if lo == hi else np.linspace(lo, hi, _BINS + 1)
    return {
        "count": n,
        "mean": round(float(stats["mean"]), 4),
        "std": round(_num(stats["std"]), 4),
        "min": round(lo, 4),
        "p10": round(q[0], 4),
        "p25": round(q[1], 4),
        "median": round(q[2], 4),
        "p75": round(q[3], 4),
        "p90": round(q[4], 4),
        "max": round(hi, 4),
        "histogram": [
            {"lo": round(float(edges[i]), 4), "hi": round(float(edges[i + 1]), 4),
             "count": int(hist.get(i, 0))}
            for i in range(_BINS)
        ],
    }


def _score_windows(cfg: PlatformConfig, tenant_id: str, table: str,
                   metrics: list[str]) -> dict[str, Any] | None:
    """Per-window (last N days, relative to the tenant's latest ``scored_at``)
    distributions of ``metrics`` in ``gold.<table>``; None if the tenant has no scores.

    Without a ``scored_at`` column (or with it all null) every window falls back to
    the full snapshot with a note, as the pandas reference does.
    """
    cols = set(_q(cfg, tenant_id, table, f"SELECT * FROM gold.{table} WHERE false").columns)
    if not cols:
        return None                                       # not written yet
    exprs = {}
    for m in metrics:
        if m in cols:
            exprs[m] = f"CAST({m} AS double)"
        elif m in _DERIVED and _DERIVED[m][1] <= cols:
            exprs[m] = f"CAST({_DERIVED[m][0]} AS double)"
    has_ts_col = "scored_at" in cols
    n = _q(cfg, tenant_id, table,
           f"SELECT tenant_id, count(*) AS n{', count(scored_at) AS n_ts' if has_ts_col else ''} "
           f"FROM gold.{table} WHERE tenant_id = ? GROUP BY 1", (tenant_id,))
    if n.empty:
        return None
    has_ts = has_ts_col and int(n["n_ts"].iloc[0]) > 0

    # The tenant's rows once per window, then one (window, metric, value) row per
    # non-null value; the `_rows` pseudo-metric (always 0) counts each window's rows.
    windows = ", ".join(str(int(w)) for w in _SCORE_WINDOWS)
    in_window = ("WHERE s.scored_at >= date_add('day', -w.w, (SELECT max(scored_at) FROM s))"
                 if has_ts else "")
    names = ", ".join(f"'{m}'" for m in ("_rows", *exprs))
    values = ", ".join(("0e0", *exprs.values()))
    base = (f"s AS (SELECT * FROM gold.{table} WHERE tenant_id = ?), "
            f"w AS (SELECT w FROM UNNEST(ARRAY[{windows}]) AS t(w)), "
            f"x AS (SELECT w.w, s.* FROM s CROSS JOIN w {in_window}), "
            "l AS (SELECT x.tenant_id, x.w, m.metric, m.v FROM x "
            f"CROSS JOIN UNNEST(ARRAY[{names}], ARRAY[{values}]) AS m(metric, v) "
            "WHERE m.v IS NOT NULL)")
    # Exact percentiles: the two order statistics around each quantile's position
    # (numbered within the window and metric); Python interpolates them as numpy does.
    order_stats = ", ".join(_order_stat_sql(q, k) for k, q in enumerate(_QUANTILES))
    stats = _q(cfg, tenant_id, table,
               f"WITH {base}, "
               "o AS (SELECT l.*, row_number() OVER (PARTITION BY w, metric ORDER BY v) - 1 AS i, "
               "count(*) OVER (PARTITION BY w, metric) AS cnt FROM l) "
               "SELECT tenant_id, w, metric, count(*) AS n, avg(v) AS mean, stddev_pop(v) AS std, "
               f"min(v) AS lo, max(v) AS hi, {order_stats} FROM o GROUP BY 1, 2, 3", (tenant_id,))

    hists: dict[tuple[int, str], dict[int, int]] = {}
    if exprs:
        # np.histogram's bins: 8 equal bins over [min, max] (widened by 0.5 when
        # min == max), the last one closed; the bin index is corrected against the
        # edges exactly as numpy does, so the counts are equal.
        hist = _q(cfg, tenant_id, table,
                  f"WITH {base}, "
                  "r AS (SELECT w, metric, "
                  "CASE WHEN min(v) = max(v) THEN min(v) - 0.5e0 ELSE min(v) END AS lo, "
                  "CASE WHEN min(v) = max(v) THEN max(v) + 0.5e0 ELSE max(v) END AS hi "
                  "FROM l WHERE metric <> '_rows' GROUP BY 1, 2), "
                  "b AS (SELECT l.tenant_id, l.w, l.metric, l.v, r.lo, r.hi, "
                  f"least(CAST(floor((l.v - r.lo) / (r.hi - r.lo) * {_BINS}e0) AS bigint), {_BINS - 1}) AS b0 "
                  "FROM l JOIN r ON l.w = r.w AND l.metric = r.metric), "
                  "e AS (SELECT b.*, "
                  f"b0 * ((hi - lo) / {_BINS}e0) + lo AS e_lo, "
                  f"CASE WHEN b0 + 1 >= {_BINS} THEN hi ELSE (b0 + 1) * ((hi - lo) / {_BINS}e0) + lo END AS e_hi "
                  "FROM b) "
                  "SELECT tenant_id, w, metric, CASE WHEN v < e_lo THEN b0 - 1 "
                  f"WHEN b0 <> {_BINS - 1} AND v >= e_hi THEN b0 + 1 ELSE b0 END AS bin, "
                  "count(*) AS n FROM e GROUP BY 1, 2, 3, 4", (tenant_id,))
        for r in hist.itertuples():
            hists.setdefault((int(r.w), str(r.metric)), {})[int(r.bin)] = int(r.n)

    by_wm = {(int(r["w"]), str(r["metric"])): r for _, r in stats.iterrows()}
    out: dict[str, Any] = {}
    for w in _SCORE_WINDOWS:
        rows = by_wm.get((int(w), "_rows"))
        block: dict[str, Any] = {"count": 0 if rows is None else int(rows["n"])}
        if not has_ts:
            block["note"] = "no scored_at timestamp; showing the full snapshot"
        for m in exprs:
            block[m] = _distribution(by_wm.get((int(w), m)), hists.get((int(w), m), {}))
        out[f"{int(w)}d"] = block
    return out


# --------------------------------------------------------------------------- #
# Tool 6 — fraud score distributions                                           #
# --------------------------------------------------------------------------- #
@_lake_guard
def analyze_fraud_score_tool(tenant_id: str, *, config_path: str | None = None) -> dict[str, Any]:
    """Fraud-score distributions for one tenant over the last 7/15/30 days (REQ-MCP-07).

    Reads the Gold ``fraud_scores`` table (from `idp score`) and returns, per window,
    the distribution of the supervised probability (`fraud_proba`), the deterministic
    `rule_score`, and the fused `fraud_score`. Windows are relative to the latest
    `scored_at`; without accumulated timestamped history each window is the current
    snapshot (see note in the result).
    """
    cfg = _cfg(config_path)
    by_window = _score_windows(cfg, tenant_id, _FRAUD_SCORES,
                               ["fraud_proba", "rule_score", "fraud_score"])
    if by_window is None:
        return {"tenant_id": tenant_id, "count": 0,
                "note": "no fraud scores for tenant — run 'idp score' after training"}
    return {"tenant_id": tenant_id, "windows_days": list(_SCORE_WINDOWS), "by_window": by_window}


# --------------------------------------------------------------------------- #
# Tool 7 — credit score distributions                                          #
# --------------------------------------------------------------------------- #
@_lake_guard
def analyze_credit_score_tool(tenant_id: str, *, config_path: str | None = None) -> dict[str, Any]:
    """Credit-score distributions for one tenant over the last 7/15/30 days (REQ-MCP-08).

    Reads the Gold ``credit_scores`` table (from `idp score`) and returns, per window,
    the distribution of probability-of-default (`pd`), affordability (`affordability_base`),
    the `recommended_limit`, and the affordability-vs-limit **gap** (the risk haircut,
    `affordability_base - recommended_limit`). Windows are relative to the latest
    `scored_at`.
    """
    cfg = _cfg(config_path)
    by_window = _score_windows(cfg, tenant_id, _CREDIT_SCORES,
                               ["pd", "affordability_base", "recommended_limit", "affordability_gap"])
    if by_window is None:
        return {"tenant_id": tenant_id, "count": 0,
                "note": "no credit scores for tenant — run 'idp score' after training"}
    return {"tenant_id": tenant_id, "windows_days": list(_SCORE_WINDOWS), "by_window": by_window}


# Registry consumed by server.py (name -> callable).
TOOLS = {
    "analyze_subscribers_tool": analyze_subscribers_tool,
    "analyze_usage_events_tool": analyze_usage_events_tool,
    "analyze_recharges_tool": analyze_recharges_tool,
    "analyze_recharge_anomalies_tool": analyze_recharge_anomalies_tool,
    "analyze_fee_charges_tool": analyze_fee_charges_tool,
    "analyze_fraud_score_tool": analyze_fraud_score_tool,
    "analyze_credit_score_tool": analyze_credit_score_tool,
}
