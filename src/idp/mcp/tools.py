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

from typing import Any

import pandas as pd

from idp.common import storage
from idp.common.config import PlatformConfig, load_config

# Silver tables the tools read.
_DIM = "dim_subscriber"
_USAGE = "fact_usage"
_RECHARGE = "fact_recharge"
_FEES = "fact_fee_charge"


def _cfg(config_path: str | None) -> PlatformConfig:
    return load_config(config_path)


def _read_silver(cfg: PlatformConfig, table: str, tenant_id: str) -> pd.DataFrame:
    """Read one Silver table and return only ``tenant_id``'s rows (empty if absent)."""
    base = storage.lake_uri(cfg, "silver")
    try:
        df = storage.read_parquet(f"{base}/{table}", cfg)
    except Exception:  # noqa: BLE001 - missing/empty table -> empty frame
        return pd.DataFrame()
    if "tenant_id" not in df.columns:
        return pd.DataFrame()
    df["tenant_id"] = df["tenant_id"].astype(str)
    return df[df["tenant_id"] == str(tenant_id)].copy()


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


# --------------------------------------------------------------------------- #
# Tool 1 — subscribers                                                         #
# --------------------------------------------------------------------------- #
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


# Registry consumed by server.py (name -> callable).
TOOLS = {
    "analyze_subscribers_tool": analyze_subscribers_tool,
    "analyze_usage_events_tool": analyze_usage_events_tool,
    "analyze_recharges_tool": analyze_recharges_tool,
    "analyze_recharge_anomalies_tool": analyze_recharge_anomalies_tool,
    "analyze_fee_charges_tool": analyze_fee_charges_tool,
}
