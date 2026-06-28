"""Credit-limit feature set (plan section 4.2).

Behavioural alternative-credit signals: recharge ability & reliability, spend
capacity, account stability. Produces one row per ``(tenant_id, service_id)``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from idp.features.base import KEY, merge_features, recharge_aggregates, usage_aggregates

# Numeric features consumed by the credit model.
CREDIT_FEATURES = [
    "account_tenure_days", "num_products", "has_device_agreement", "is_company",
    "recharge_count", "recharge_success_rate", "recharge_avg_amount", "recharge_max",
    "recharge_total", "recharge_total_30d", "recharge_total_90d", "failed_recharge_count",
    "days_since_last_recharge", "share_credit_card",
    "usage_charged_amount", "usage_charged_amount_30d", "voice_minutes", "data_bytes",
    "arpu_monthly", "spend_to_recharge_ratio",
]


def build_credit_features(silver: dict[str, pd.DataFrame], windows=(30, 90)) -> pd.DataFrame:
    """Assemble the credit feature table from Silver tables."""
    dim = silver["dim_subscriber"].copy()
    rech = recharge_aggregates(silver["fact_recharge"], windows)
    usage = usage_aggregates(silver["fact_usage"], windows)
    feats = merge_features(dim, rech, usage)

    feats["has_device_agreement"] = feats["has_device_agreement"].astype(int)
    feats["is_company"] = feats.get("is_company", 0).astype(int)
    # derived
    months = (feats["account_tenure_days"].clip(lower=30) / 30.0)
    feats["arpu_monthly"] = (feats["usage_charged_amount"] / months).round(4)
    feats["spend_to_recharge_ratio"] = (
        feats["usage_charged_amount"] / feats["recharge_total"].replace(0, np.nan)
    ).fillna(0.0).round(4)

    for col in CREDIT_FEATURES:
        if col not in feats.columns:
            feats[col] = 0.0
    return feats[KEY + CREDIT_FEATURES].copy()
