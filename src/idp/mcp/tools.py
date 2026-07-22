"""Read-only, tenant-scoped analytics over the Silver lake (REQ-MCP-01…06).

Five pure functions — one per business-question set — that read a tenant's Silver
tables with the standard ``common.storage`` pandas helpers and return compact,
JSON-able aggregates. They never write to the lake, never cross tenants, and only
ever emit the tokenized ``service_id`` (no raw MSISDN/IMSI). The MCP server
(``server.py``) registers these as tools; they are equally callable in tests.

Data source rationale: pandas over Silver (not live Trino) keeps the analytics
surface unit-testable without a cluster and consistent with the rest of the
prototype — see ADR-0021.
"""
from __future__ import annotations

import functools
from typing import Any

import numpy as np
import pandas as pd

from idp.common import storage
from idp.common.config import PlatformConfig, load_config

# Silver tables the tools read.
_DIM = "dim_subscriber"
_USAGE = "fact_usage"
_RECHARGE = "fact_recharge"
_FEES = "fact_fee_charge"
# Gold score tables (produced by `idp score`).
_CREDIT_SCORES = "credit_scores"
_FRAUD_SCORES = "fraud_scores"
_SCORE_WINDOWS = (7, 15, 30)  # days


def _cfg(config_path: str | None) -> PlatformConfig:
    return load_config(config_path)


class LakeReadError(Exception):
    """The Silver lake could not be reached/read (vs the table simply being absent).

    Raised for connectivity/permission failures so tools surface a clear error
    instead of silently returning an empty ``0`` result — e.g. the MinIO
    port-forward is down, or the endpoint/credentials are wrong.
    """


def _read_layer_table(cfg: PlatformConfig, layer: str, table: str, tenant_id: str) -> pd.DataFrame:
    """Read one table from a lake layer ('silver'|'gold'), tenant-scoped.

    Returns an empty frame when the table is genuinely absent (nothing written
    yet). Raises :class:`LakeReadError` when the lake can't be reached at all, so
    a broken connection is not mistaken for "no data".
    """
    uri = f"{storage.lake_uri(cfg, layer)}/{table}"
    try:
        df = storage.read_parquet(uri, cfg)
    except FileNotFoundError:
        return pd.DataFrame()  # table not written yet -> genuinely empty
    except Exception as e:  # noqa: BLE001 - connectivity/permission/etc.
        raise LakeReadError(f"could not read {uri}: {type(e).__name__}: {e}") from e
    if "tenant_id" not in df.columns:
        return pd.DataFrame()
    df["tenant_id"] = df["tenant_id"].astype(str)
    return df[df["tenant_id"] == str(tenant_id)].copy()


def _read_silver(cfg: PlatformConfig, table: str, tenant_id: str) -> pd.DataFrame:
    return _read_layer_table(cfg, "silver", table, tenant_id)


def _read_gold(cfg: PlatformConfig, table: str, tenant_id: str) -> pd.DataFrame:
    return _read_layer_table(cfg, "gold", table, tenant_id)


def _available_tenants(cfg: PlatformConfig) -> list[str]:
    """Distinct ``tenant_id`` values present in Silver (empty if nothing ingested)."""
    uri = f"{storage.lake_uri(cfg, 'silver')}/{_DIM}"
    try:
        df = storage.read_parquet(uri, cfg)
    except FileNotFoundError:
        return []
    except Exception as e:  # noqa: BLE001 - connectivity/permission/etc.
        raise LakeReadError(f"could not read {uri}: {type(e).__name__}: {e}") from e
    if "tenant_id" not in df.columns:
        return []
    return sorted(df["tenant_id"].astype(str).unique().tolist())


def _lake_guard(fn):
    """Validate the tenant and turn lake failures into structured results (never raise).

    Keeps the read-only contract and makes the two ambiguous cases explicit:

    * **Unreachable lake** -> ``{"error", "source"}`` (not a misleading empty ``0``).
    * **Missing/unknown tenant_id** -> the list of tenants that actually exist plus
      an ``action_required`` telling the client to ASK the user which tenant rather
      than guess. This is what turns "no tenant given" into a blocking question in
      the client instead of a silent wrong-tenant query.
    """
    @functools.wraps(fn)
    def wrapper(tenant_id: str = "", **kwargs):
        cfg = _cfg(kwargs.get("config_path"))
        try:
            tenants = _available_tenants(cfg)
            if not tenants:
                return {"tenant_id": tenant_id or None,
                        "error": "no tenant data found in the Silver lake "
                                 "(nothing ingested yet, or wrong endpoint/lake)",
                        "source": storage.lake_uri(cfg, "silver"),
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
                    "source": storage.lake_uri(cfg, "silver")}
    return wrapper


def _period(series: pd.Series, granularity: str) -> pd.Series:
    """Bucket a datetime series into day/week/month/quarter period labels."""
    ts = pd.to_datetime(series, utc=True, errors="coerce")
    g = (granularity or "month").lower()
    if g == "day":
        return ts.dt.strftime("%Y-%m-%d")
    if g == "week":
        return ts.dt.strftime("%G-W%V")
    if g == "quarter":
        return ts.dt.year.astype("Int64").astype(str) + "-Q" + ts.dt.quarter.astype("Int64").astype(str)
    return ts.dt.strftime("%Y-%m")  # month (default)


def _round_map(d: dict, ndigits: int = 2) -> dict:
    return {k: round(float(v), ndigits) for k, v in d.items()}


def _distribution(series: pd.Series, bins: int = 8) -> dict[str, Any]:
    """Compact numeric distribution: count, summary stats, percentiles, histogram."""
    s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
    if s.empty:
        return {"count": 0}
    qs = s.quantile([0.10, 0.25, 0.50, 0.75, 0.90])
    counts, edges = np.histogram(s.to_numpy(), bins=bins)
    return {
        "count": int(s.size),
        "mean": round(float(s.mean()), 4),
        "std": round(float(s.std(ddof=0)), 4),
        "min": round(float(s.min()), 4),
        "p10": round(float(qs.loc[0.10]), 4),
        "p25": round(float(qs.loc[0.25]), 4),
        "median": round(float(qs.loc[0.50]), 4),
        "p75": round(float(qs.loc[0.75]), 4),
        "p90": round(float(qs.loc[0.90]), 4),
        "max": round(float(s.max()), 4),
        "histogram": [
            {"lo": round(float(edges[i]), 4), "hi": round(float(edges[i + 1]), 4),
             "count": int(counts[i])}
            for i in range(len(counts))
        ],
    }


def _windowed_distributions(df: pd.DataFrame, metrics: list[str],
                            windows=_SCORE_WINDOWS) -> dict[str, Any]:
    """Per-window (last N days, relative to the latest scored_at) distributions.

    If the score table has no ``scored_at`` (older data, or a single un-timestamped
    write), every window falls back to the full snapshot with a note — meaningful
    time windows require accumulated, timestamped score history (multiple `idp
    score` runs over days).
    """
    out: dict[str, Any] = {}
    has_ts = "scored_at" in df.columns and df["scored_at"].notna().any()
    ts = pd.to_datetime(df["scored_at"], utc=True, errors="coerce") if has_ts else None
    ref = ts.max() if has_ts else None
    for w in windows:
        if has_ts:
            sub = df[ts >= ref - pd.Timedelta(days=int(w))]
        else:
            sub = df
        block: dict[str, Any] = {"count": int(len(sub))}
        if not has_ts:
            block["note"] = "no scored_at timestamp; showing the full snapshot"
        for m in metrics:
            if m in sub.columns:
                block[m] = _distribution(sub[m])
        out[f"{int(w)}d"] = block
    return out


# --------------------------------------------------------------------------- #
# Tool 1 — subscribers                                                         #
# --------------------------------------------------------------------------- #
@_lake_guard
def analyze_subscribers_tool(tenant_id: str, *, config_path: str | None = None) -> dict[str, Any]:
    """Subscriber base analytics for one tenant (REQ-MCP-01):
    subscription vs device counts, business-line mix, roamer vs non-roamer split.
    """
    cfg = _cfg(config_path)
    dim = _read_silver(cfg, _DIM, tenant_id)
    if dim.empty:
        return {"tenant_id": tenant_id, "subscriber_count": 0, "note": "no subscribers for tenant"}

    subscriber_count = int(len(dim))
    is_device = dim.get("has_device_agreement", pd.Series(False, index=dim.index)).fillna(False).astype(bool)
    device_count = int(is_device.sum())
    subscription_count = subscriber_count - device_count

    business_line_mix = (
        dim.get("business_line", pd.Series(dtype=str)).fillna("unknown").value_counts().astype(int).to_dict()
    )

    # Roamer = subscriber with >=1 roaming usage event.
    usage = _read_silver(cfg, _USAGE, tenant_id)
    roamer_ids: set = set()
    if not usage.empty and "is_roaming" in usage.columns:
        roaming = usage[usage["is_roaming"].fillna(False).astype(bool)]
        roamer_ids = set(roaming["service_id"].unique())
    roamers = int(dim["service_id"].isin(roamer_ids).sum())

    return {
        "tenant_id": tenant_id,
        "subscriber_count": subscriber_count,
        "subscription_count": subscription_count,
        "device_count": device_count,
        "business_line_mix": business_line_mix,
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
    usage = _read_silver(cfg, _USAGE, tenant_id)
    if usage.empty:
        return {"tenant_id": tenant_id, "total_events": 0, "note": "no usage events for tenant"}

    amount_col = "gross_amount" if "gross_amount" in usage.columns else "net_amount"
    revenue_by_event_type = _round_map(
        usage.groupby("event_type")[amount_col].sum().to_dict(), 4
    )
    counts_by_event_type = usage.groupby("event_type").size().astype(int).to_dict()

    duration_by_event_type: dict[str, Any] = {}
    for etype, part in usage.groupby("event_type"):
        unit = part["unit"].dropna().iloc[0] if "unit" in part and part["unit"].notna().any() else None
        duration_by_event_type[str(etype)] = {
            "total_qty": int(part["qty"].fillna(0).sum()) if "qty" in part else 0,
            "unit": unit,
        }

    period = _period(usage["started_at"], granularity)
    ts = (
        usage.assign(_period=period)
        .groupby(["_period", "event_type"])
        .agg(events=("id", "size"), revenue=(amount_col, "sum"))
        .reset_index()
    )
    usage_over_time = [
        {
            "period": r["_period"],
            "event_type": r["event_type"],
            "events": int(r["events"]),
            "revenue": round(float(r["revenue"]), 4),
        }
        for r in ts.to_dict("records")
        if pd.notna(r["_period"])
    ]

    return {
        "tenant_id": tenant_id,
        "granularity": granularity,
        "total_events": int(len(usage)),
        "counts_by_event_type": {k: int(v) for k, v in counts_by_event_type.items()},
        "revenue_by_event_type": revenue_by_event_type,
        "duration_by_event_type": duration_by_event_type,
        "usage_over_time": usage_over_time,
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
    rech = _read_silver(cfg, _RECHARGE, tenant_id)
    if rech.empty:
        return {"tenant_id": tenant_id, "recharge_count": 0, "note": "no recharges for tenant"}

    ok = rech[rech.get("status", "successful").astype(str) == "successful"] if "status" in rech else rech
    total_topup = float(ok["amount"].sum())
    successful_count = int(len(ok))
    recharge_count = int(len(rech))
    average_topup = round(total_topup / successful_count, 2) if successful_count else 0.0

    def _split(col: str) -> dict[str, Any]:
        if col not in rech.columns:
            return {}
        return {
            "count": {k: int(v) for k, v in rech[col].fillna("unknown").value_counts().to_dict().items()},
            "amount": _round_map(ok.groupby(col)["amount"].sum().to_dict(), 2) if not ok.empty else {},
        }

    period = _period(rech["created_at"], granularity)
    by_time = (
        ok.assign(_period=period.loc[ok.index] if not ok.empty else period)
        .groupby("_period")
        .agg(amount=("amount", "sum"), count=("id", "size"))
        .reset_index()
    )
    by_time_list = [
        {"period": r["_period"], "amount": round(float(r["amount"]), 2), "count": int(r["count"])}
        for r in by_time.to_dict("records")
        if pd.notna(r["_period"])
    ]

    return {
        "tenant_id": tenant_id,
        "granularity": granularity,
        "total_topup": round(total_topup, 2),
        "recharge_count": recharge_count,
        "successful_count": successful_count,
        "average_topup": average_topup,
        "by_recharge_type": _split("recharge_type"),
        "by_payment_method": _split("payment_method"),
        "by_time": by_time_list,
    }


# --------------------------------------------------------------------------- #
# Tool 4 — recharge anomalies                                                  #
# --------------------------------------------------------------------------- #
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
    rech = _read_silver(cfg, _RECHARGE, tenant_id)
    if rech.empty:
        return {"tenant_id": tenant_id, "amount_spikes": [], "volume_spikes": [], "note": "no recharges"}

    ok = rech[rech.get("status", "successful").astype(str) == "successful"] if "status" in rech else rech
    ok = ok.copy()
    ok["day"] = pd.to_datetime(ok["created_at"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d")

    # (a) amount spikes — robust z-score = 0.6745*(x - median)/MAD
    amount_spikes: list[dict] = []
    amt = ok["amount"].astype(float)
    med = amt.median()
    mad = (amt - med).abs().median()
    if mad > 0:
        rz = 0.6745 * (amt - med) / mad
        flagged = ok[rz >= sensitivity].assign(robust_z=rz[rz >= sensitivity].round(2))
        amount_spikes = [
            {"service_id": r["service_id"], "day": r["day"], "amount": round(float(r["amount"]), 2),
             "robust_z": float(r["robust_z"])}
            for r in flagged.sort_values("amount", ascending=False).head(limit).to_dict("records")
        ]

    # (b)/(c) subscriber-day volume — count recharges per (service_id, day)
    sd = ok.groupby(["service_id", "day"]).agg(recharge_count=("id", "size"),
                                               total_amount=("amount", "sum")).reset_index()
    vc = sd["recharge_count"].astype(float)
    vol_threshold = max(2.0, float(vc.mean() + sensitivity * vc.std(ddof=0))) if len(vc) else 2.0
    volume_spikes = [
        {"service_id": r["service_id"], "day": r["day"], "recharge_count": int(r["recharge_count"]),
         "total_amount": round(float(r["total_amount"]), 2)}
        for r in sd[vc >= vol_threshold].sort_values("recharge_count", ascending=False)
        .head(limit).to_dict("records")
    ]
    anomalous_days = {(r["service_id"], r["day"]) for r in volume_spikes}
    anomalous_days |= {(r["service_id"], r["day"]) for r in amount_spikes}
    subscriber_day_anomalies = [{"service_id": s, "day": d} for s, d in sorted(anomalous_days)][:limit]

    # (d) periodic view
    gran = "week" if str(view).lower().startswith("week") else "month"
    period = _period(ok["created_at"], gran)
    per = (
        ok.assign(_period=period, _sd=ok["service_id"].astype(str) + "|" + ok["day"].astype(str))
        .groupby("_period")
        .agg(total_amount=("amount", "sum"), recharge_count=("id", "size"))
        .reset_index()
    )
    anom_by_period: dict[str, int] = {}
    for s, d in anomalous_days:
        p = _period(pd.Series([d]), gran).iloc[0]
        anom_by_period[p] = anom_by_period.get(p, 0) + 1
    periodic_view = [
        {"period": r["_period"], "total_amount": round(float(r["total_amount"]), 2),
         "recharge_count": int(r["recharge_count"]),
         "anomalous_subscriber_days": int(anom_by_period.get(r["_period"], 0))}
        for r in per.to_dict("records") if pd.notna(r["_period"])
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
    fees = _read_silver(cfg, _FEES, tenant_id)
    if fees.empty:
        return {"tenant_id": tenant_id, "total_fee_revenue": 0.0, "note": "no fee charges for tenant"}

    total = float(fees["amount"].sum())
    rev_by_product = fees.groupby("product_name")["amount"].sum()
    revenue_by_product = _round_map(rev_by_product.to_dict(), 2)
    # revenue share sums to ~1.0 (net of discounts); guard divide-by-zero
    share_base = rev_by_product.sum()
    revenue_share_by_product = (
        _round_map((rev_by_product / share_base).to_dict(), 4) if share_base else {}
    )

    def _by(col: str) -> dict[str, Any]:
        if col not in fees.columns:
            return {}
        grp = fees.groupby(col)
        return {
            "revenue": _round_map(grp["amount"].sum().to_dict(), 2),
            "count": {k: int(v) for k, v in grp.size().to_dict().items()},
        }

    gran = "quarter" if str(breakdown).lower().startswith("q") else "month"
    period = _period(fees["charged_at"], gran)
    by_period = (
        fees.assign(_period=period)
        .groupby(["_period", "category"])
        .agg(revenue=("amount", "sum"), count=("id", "size"))
        .reset_index()
    )
    period_breakdown = [
        {"period": r["_period"], "category": r["category"],
         "revenue": round(float(r["revenue"]), 2), "count": int(r["count"])}
        for r in by_period.to_dict("records") if pd.notna(r["_period"])
    ]

    return {
        "tenant_id": tenant_id,
        "breakdown": gran,
        "total_fee_revenue": round(total, 2),
        "revenue_by_product": revenue_by_product,
        "revenue_share_by_product": revenue_share_by_product,
        "by_charge_type": _by("charge_type"),
        "by_category": _by("category"),
        "period_breakdown": period_breakdown,
    }


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
    scores = _read_gold(cfg, _FRAUD_SCORES, tenant_id)
    if scores.empty:
        return {"tenant_id": tenant_id, "count": 0,
                "note": "no fraud scores for tenant — run 'idp score' after training"}
    return {
        "tenant_id": tenant_id,
        "windows_days": list(_SCORE_WINDOWS),
        "by_window": _windowed_distributions(
            scores, ["fraud_proba", "rule_score", "fraud_score"]),
    }


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
    scores = _read_gold(cfg, _CREDIT_SCORES, tenant_id)
    if scores.empty:
        return {"tenant_id": tenant_id, "count": 0,
                "note": "no credit scores for tenant — run 'idp score' after training"}
    if {"affordability_base", "recommended_limit"} <= set(scores.columns):
        scores = scores.assign(
            affordability_gap=scores["affordability_base"].astype(float)
            - scores["recommended_limit"].astype(float))
    return {
        "tenant_id": tenant_id,
        "windows_days": list(_SCORE_WINDOWS),
        "by_window": _windowed_distributions(
            scores, ["pd", "affordability_base", "recommended_limit", "affordability_gap"]),
    }


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
