"""Gold feature engineering: Silver -> credit/fraud feature tables, on Spark (plan 4).

Pure DataFrame functions. Each feature table is keyed by ``(tenant_id,
service_id)`` — one row per subscriber in ``dim_subscriber`` who granted analytics
consent (REQ-FND-05) — followed by exactly the model feature contract (``idp.features.CREDIT_FEATURES`` /
``FRAUD_FEATURES``).

Lookback windows use one reference time per source table: that table's latest
timestamp across all tenants. A missing aggregate (an empty window, or a subscriber
without recharges) is 0.
"""
from __future__ import annotations

from typing import Any

from idp.common.pii import ConsentError
from idp.features import CREDIT_FEATURES, FRAUD_FEATURES

KEY = ["tenant_id", "service_id"]
DAY_US = 86_400 * 1_000_000


def consenting_subscribers(dim: Any, purpose: str = "analytics") -> Any:
    """Subscribers who granted consent for ``purpose`` (REQ-FND-05).

    Fail closed: a false or null ``consent_<purpose>`` excludes the subscriber, and
    a dimension without the column raises :class:`ConsentError` rather than
    passing everyone through.
    """
    from pyspark.sql import functions as F

    col = f"consent_{purpose}"
    if col not in dim.columns:
        raise ConsentError(
            f"dim_subscriber has no {col!r} column; cannot decide consent for "
            f"{purpose!r} processing. Rebuild Silver ('idp silver') first."
        )
    return dim.filter(F.coalesce(F.col(col).cast("boolean"), F.lit(False)))


def _ref_us(df: Any, col: str) -> int | None:
    """Latest ``col`` over the whole table, in epoch microseconds."""
    from pyspark.sql import functions as F
    return df.agg(F.max(F.unix_micros(F.col(col)))).collect()[0][0]


def _when(value: Any, cond: Any) -> Any:
    from pyspark.sql import functions as F
    return F.when(cond, value)


def _count_if(cond: Any) -> Any:
    from pyspark.sql import functions as F
    return F.sum(F.when(cond, 1).otherwise(0))


def recharge_aggregates(recharge: Any, windows) -> Any:
    """Per-subscriber recharge ability/reliability aggregates (plan 4.2)."""
    from pyspark.sql import functions as F

    ref = _ref_us(recharge, "created_at")
    ts = F.unix_micros(F.col("created_at"))
    ok = F.col("status") == "successful"
    total = F.count(F.lit(1))
    aggs = [
        total.alias("recharge_count"),
        _count_if(F.col("status") == "failed").alias("failed_recharge_count"),
        (_count_if(ok) / total).alias("recharge_success_rate"),
        F.avg(_when(F.col("amount"), ok)).alias("recharge_avg_amount"),
        F.max(_when(F.col("amount"), ok)).alias("recharge_max"),
        F.sum(_when(F.col("amount"), ok)).alias("recharge_total"),
        F.floor((F.lit(ref) - F.max(ts)) / F.lit(DAY_US)).alias("days_since_last_recharge"),
        (_count_if(F.col("payment_method") == "credit_card") / total).alias("share_credit_card"),
    ]
    for w in windows:
        in_window = ok & (ts >= F.lit(ref - w * DAY_US))
        aggs += [F.sum(_when(F.col("amount"), in_window)).alias(f"recharge_total_{w}d"),
                 _count_if(in_window).alias(f"recharge_count_{w}d")]
    return recharge.groupBy(*KEY).agg(*aggs).na.fill(0)


def usage_aggregates(usage: Any, windows) -> Any:
    """Per-subscriber usage/spend and velocity aggregates (plan 4.2/4.3)."""
    from pyspark.sql import functions as F

    ref = _ref_us(usage, "started_at")
    ts = F.unix_micros(F.col("started_at"))
    voice = F.col("event_type") == "voice"
    data = F.col("event_type") == "data"
    roaming = F.col("is_roaming") == F.lit(True)
    voice_calls = _count_if(voice)
    total = F.count(F.lit(1))
    aggs = [
        total.alias("usage_event_count"),
        F.sum("gross_amount").alias("usage_charged_amount"),
        (F.sum(_when(F.col("qty"), voice)) / F.lit(60.0)).alias("voice_minutes"),
        voice_calls.alias("voice_call_count"),
        F.sum(_when(F.col("qty"), data)).alias("data_bytes"),
        _count_if(roaming).alias("roaming_event_count"),
        (_count_if(roaming) / total).alias("roaming_share"),
        # SIM-box signal: share of originating voice calls
        (_count_if(voice & (F.col("traffic_case") == "originating")) / voice_calls).alias("originating_ratio"),
        F.countDistinct(_when(F.col("other_party_number"), voice)).alias("unique_called_numbers"),
        # calls per active (UTC) day
        (voice_calls / F.countDistinct(_when(F.to_date("started_at"), voice))).alias("calls_per_active_day"),
    ]
    for w in windows:
        in_window = ts >= F.lit(ref - w * DAY_US)
        aggs += [F.sum(_when(F.col("gross_amount"), in_window)).alias(f"usage_charged_amount_{w}d"),
                 F.sum(_when(F.col("qty"), in_window & data)).alias(f"data_bytes_{w}d")]
    return usage.groupBy(*KEY).agg(*aggs).na.fill(0)


def fee_aggregates(fees: Any | None, dim: Any, windows) -> Any | None:
    """Per-subscriber product fee-charge aggregates (ADR-0023).

    Fees reference a product, so they are attributed to a subscriber via
    ``agreement_id -> service_id`` from the subscriber dimension.
    """
    from pyspark.sql import functions as F

    if fees is None or "agreement_id" not in fees.columns:
        return None
    link = dim.select("tenant_id", "agreement_id", "service_id").distinct()
    df = fees.join(link, ["tenant_id", "agreement_id"], "inner")
    ref = _ref_us(df, "charged_at")
    if ref is None:
        return None
    ts = F.unix_micros(F.col("charged_at"))
    ct = F.col("charge_type")
    aggs = [
        F.count(F.lit(1)).alias("fee_charge_count"),
        F.sum("amount").alias("fee_total"),
        F.avg("amount").alias("fee_avg_amount"),
        F.sum(_when(F.col("amount"), ct == "recurring")).alias("fee_recurring_total"),
        _count_if(ct == "overage").alias("fee_overage_count"),
        F.sum(_when(F.col("amount"), ct == "discount")).alias("fee_discount_total"),
    ]
    for w in windows:
        aggs.append(F.sum(_when(F.col("amount"), ts >= F.lit(ref - w * DAY_US))).alias(f"fee_total_{w}d"))
    return df.groupBy(*KEY).agg(*aggs).na.fill(0)


def _merge(dim: Any, *aggregates: Any) -> Any:
    """dim LEFT JOIN each aggregate; missing numeric aggregates -> 0."""
    out = dim
    for agg in aggregates:
        if agg is not None:
            out = out.join(agg, KEY, "left")
    return out.na.fill(0)


def _select(df: Any, features: list[str]) -> Any:
    """Keys + the feature contract, in order (absent features -> 0.0)."""
    from pyspark.sql import functions as F
    cols = [F.col(c) if c in df.columns else F.lit(0.0).alias(c) for c in features]
    return df.select(*KEY, *cols)


def build_credit_features(silver: dict[str, Any], windows=(30, 90)) -> Any:
    """Credit feature table (plan 4.2): recharge ability/reliability, spend, tenure, fees.

    Only subscribers with analytics consent get a row (REQ-FND-05).
    """
    from pyspark.sql import functions as F

    dim = consenting_subscribers(silver["dim_subscriber"])
    feats = _merge(dim,
                   recharge_aggregates(silver["fact_recharge"], windows),
                   usage_aggregates(silver["fact_usage"], windows),
                   fee_aggregates(silver.get("fact_fee_charge"), dim, windows))
    feats = (feats
             .withColumn("has_device_agreement",
                         F.coalesce(F.col("has_device_agreement").cast("int"), F.lit(0)))
             .withColumn("is_company", F.coalesce(F.col("is_company").cast("int"), F.lit(0))))
    months = F.greatest(F.col("account_tenure_days"), F.lit(30)) / F.lit(30.0)
    feats = feats.withColumn("arpu_monthly", F.bround(F.col("usage_charged_amount") / months, 4))
    feats = feats.withColumn(
        "spend_to_recharge_ratio",
        F.when(F.col("recharge_total") == 0, F.lit(0.0))
         .otherwise(F.bround(F.col("usage_charged_amount") / F.col("recharge_total"), 4)),
    )
    return _select(feats, CREDIT_FEATURES)


def build_fraud_features(silver: dict[str, Any], windows=(30, 90)) -> Any:
    """Fraud feature table (plan 4.3): velocity, traffic asymmetry, roaming, recharge anomalies.

    Only subscribers with analytics consent get a row (REQ-FND-05).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    feats = _merge(consenting_subscribers(silver["dim_subscriber"]),
                   recharge_aggregates(silver["fact_recharge"], windows),
                   usage_aggregates(silver["fact_usage"], windows))
    # per-tenant population z-score of the max recharge (card-fraud signal)
    by_tenant = Window.partitionBy("tenant_id")
    std = F.stddev_pop("recharge_max").over(by_tenant)
    feats = feats.withColumn(
        "recharge_amount_zscore",
        F.when(std.isNull() | (std == 0), F.lit(0.0))
         .otherwise((F.col("recharge_max") - F.avg("recharge_max").over(by_tenant)) / std),
    )
    return _select(feats, FRAUD_FEATURES)
