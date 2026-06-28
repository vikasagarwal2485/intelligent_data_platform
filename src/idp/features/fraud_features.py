"""Fraud feature set (plan section 4.3).

Targets SIM-box, recharge/card fraud and roaming fraud via velocity, traffic
asymmetry, roaming intensity and recharge-failure signals. One row per
``(tenant_id, service_id)``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from idp.features.base import KEY, merge_features, recharge_aggregates, usage_aggregates

FRAUD_FEATURES = [
    "calls_per_active_day", "originating_ratio", "unique_called_numbers",
    "voice_call_count", "data_bytes", "roaming_share", "roaming_event_count",
    "usage_charged_amount", "recharge_count", "failed_recharge_count",
    "recharge_success_rate", "recharge_max", "share_credit_card",
    "account_tenure_days", "recharge_amount_zscore",
]


def build_fraud_features(silver: dict[str, pd.DataFrame], windows=(30, 90)) -> pd.DataFrame:
    """Assemble the fraud feature table from Silver tables."""
    dim = silver["dim_subscriber"].copy()
    rech = recharge_aggregates(silver["fact_recharge"], windows)
    usage = usage_aggregates(silver["fact_usage"], windows)
    feats = merge_features(dim, rech, usage)

    # population z-score of max recharge within each tenant (card-fraud signal)
    def _zscore(g: pd.Series) -> pd.Series:
        std = g.std(ddof=0)
        return (g - g.mean()) / std if std and not np.isnan(std) else g * 0.0

    if "recharge_max" in feats.columns:
        feats["recharge_amount_zscore"] = (
            feats.groupby("tenant_id")["recharge_max"].transform(_zscore).fillna(0.0)
        )
    else:
        feats["recharge_amount_zscore"] = 0.0

    for col in FRAUD_FEATURES:
        if col not in feats.columns:
            feats[col] = 0.0
    return feats[KEY + FRAUD_FEATURES].copy()
