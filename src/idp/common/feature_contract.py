"""Model feature contract (plan section 4).

The ordered feature columns shared by three layers: the Gold feature tables
(written by ``idp.transforms.features``) are keyed by ``(tenant_id, service_id)``
followed by exactly these columns; the credit and fraud models are trained and
scored on them (``idp.ml``); and the scoring API builds its request frames from
them (``idp.serving.api``).
"""

# Numeric features consumed by the credit model.
CREDIT_FEATURES = [
    "account_tenure_days", "num_products", "has_device_agreement", "is_company",
    "recharge_count", "recharge_success_rate", "recharge_avg_amount", "recharge_max",
    "recharge_total", "recharge_total_30d", "recharge_total_90d", "failed_recharge_count",
    "days_since_last_recharge", "share_credit_card",
    "usage_charged_amount", "usage_charged_amount_30d", "voice_minutes", "data_bytes",
    "arpu_monthly", "spend_to_recharge_ratio",
    # product fee-charge signals (spend commitment) — ADR-0023
    "fee_total", "fee_recurring_total", "fee_overage_count", "fee_charge_count", "fee_total_30d",
]

# Numeric features consumed by the fraud model.
FRAUD_FEATURES = [
    "calls_per_active_day", "originating_ratio", "unique_called_numbers",
    "voice_call_count", "data_bytes", "roaming_share", "roaming_event_count",
    "usage_charged_amount", "recharge_count", "failed_recharge_count",
    "recharge_success_rate", "recharge_max", "share_credit_card",
    "account_tenure_days", "recharge_amount_zscore",
]

__all__ = ["CREDIT_FEATURES", "FRAUD_FEATURES"]
