"""Shared feature aggregation helpers (plan section 4).

Computes per-subscriber aggregates from the Silver facts over configurable
lookback windows. All features are keyed by ``(tenant_id, service_id)`` so they
remain tenant-isolated.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

KEY = ["tenant_id", "service_id"]


def _reference_time(*frames: pd.DataFrame, col: str) -> pd.Timestamp:
    times = [f[col].max() for f in frames if not f.empty and col in f.columns]
    times = [t for t in times if pd.notna(t)]
    return max(times) if times else pd.Timestamp.utcnow()


def recharge_aggregates(fact_recharge: pd.DataFrame, windows=(30, 90)) -> pd.DataFrame:
    """Per-subscriber recharge ability/reliability aggregates (plan 4.2)."""
    if fact_recharge.empty:
        return pd.DataFrame(columns=KEY)
    df = fact_recharge.copy()
    ref = _reference_time(df, col="created_at")
    ok = df[df["status"] == "successful"]

    grp_all = df.groupby(KEY)
    out = pd.DataFrame(index=grp_all.size().index)
    out["recharge_count"] = grp_all.size()
    out["failed_recharge_count"] = df[df["status"] == "failed"].groupby(KEY).size()
    out["recharge_success_rate"] = (
        ok.groupby(KEY).size() / grp_all.size()
    )
    out["recharge_avg_amount"] = ok.groupby(KEY)["amount"].mean()
    out["recharge_max"] = ok.groupby(KEY)["amount"].max()
    out["recharge_total"] = ok.groupby(KEY)["amount"].sum()
    last = grp_all["created_at"].max()
    out["days_since_last_recharge"] = (ref - last).dt.days
    out["share_credit_card"] = (
        df[df["payment_method"] == "credit_card"].groupby(KEY).size() / grp_all.size()
    )
    for w in windows:
        cutoff = ref - pd.Timedelta(days=w)
        win = ok[ok["created_at"] >= cutoff]
        out[f"recharge_total_{w}d"] = win.groupby(KEY)["amount"].sum()
        out[f"recharge_count_{w}d"] = win.groupby(KEY).size()
    out = out.fillna(0.0)
    return out.reset_index()


def usage_aggregates(fact_usage: pd.DataFrame, windows=(30, 90)) -> pd.DataFrame:
    """Per-subscriber usage/spend aggregates (plan 4.2/4.3)."""
    if fact_usage.empty:
        return pd.DataFrame(columns=KEY)
    df = fact_usage.copy()
    ref = _reference_time(df, col="started_at")
    voice = df[df["event_type"] == "voice"]
    data = df[df["event_type"] == "data"]

    out = pd.DataFrame(index=df.groupby(KEY).size().index)
    out["usage_event_count"] = df.groupby(KEY).size()
    out["usage_charged_amount"] = df.groupby(KEY)["gross_amount"].sum()
    out["voice_minutes"] = voice.groupby(KEY)["qty"].sum() / 60.0
    out["voice_call_count"] = voice.groupby(KEY).size()
    out["data_bytes"] = data.groupby(KEY)["qty"].sum()
    out["roaming_event_count"] = df[df["is_roaming"]].groupby(KEY).size()
    out["roaming_share"] = (
        df[df["is_roaming"]].groupby(KEY).size() / df.groupby(KEY).size()
    )
    # SIM-box signal: ratio of originating voice calls
    orig = voice[voice["traffic_case"] == "originating"]
    out["originating_ratio"] = (orig.groupby(KEY).size() / voice.groupby(KEY).size())
    out["unique_called_numbers"] = voice.groupby(KEY)["other_party_number"].nunique()

    span_days = max(1, (ref - _reference_time(df, col="started_at") + pd.Timedelta(days=1)).days)
    # calls per active day approximation
    call_days = voice.groupby(KEY)["started_at"].apply(lambda s: s.dt.date.nunique())
    out["calls_per_active_day"] = out["voice_call_count"] / call_days.replace(0, np.nan)

    for w in windows:
        cutoff = ref - pd.Timedelta(days=w)
        win = df[df["started_at"] >= cutoff]
        out[f"usage_charged_amount_{w}d"] = win.groupby(KEY)["gross_amount"].sum()
        out[f"data_bytes_{w}d"] = win[win["event_type"] == "data"].groupby(KEY)["qty"].sum()
    out = out.fillna(0.0)
    return out.reset_index()


def merge_features(
    dim: pd.DataFrame, *aggregates: pd.DataFrame
) -> pd.DataFrame:
    """Left-join all aggregate frames onto the subscriber dimension."""
    base = dim.copy()
    for agg in aggregates:
        if agg is None or agg.empty:
            continue
        base = base.merge(agg, on=KEY, how="left")
    return base.fillna(0.0)
