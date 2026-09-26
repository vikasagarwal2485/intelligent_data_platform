"""Production Gold stage as a PySpark job (Silver -> Gold on S3/MinIO).

This is the distributed counterpart of the on-demand `idp gold` stage: it reads
the Silver tables from object storage, computes per-subscriber feature tables,
and writes them to the Gold bucket — the same layering, on Spark, so it scales to
billions of rows (plan section 4.3).

PARITY (REQ-ORC-04): the output is column-for-column the pandas feature set
(``idp.features``): keys + ``CREDIT_FEATURES`` / ``FRAUD_FEATURES`` in the same
order, the same lookback-window semantics (each table's own max timestamp as the
reference) and the same derivations, so the trained models can score Spark-built
Gold. This script cannot import ``idp`` (it runs in the plain apache/spark image),
so the feature lists are copied below; tests/test_spark_parity.py fails if they
drift from the pandas lists.

Submit (example, against MinIO):
  spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio.idp-platform.svc:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    pipelines/spark/gold_job.py --prefix idp

On the target platform this runs via the Spark Operator / Airflow SparkSubmit,
reading Silver Iceberg tables through the Nessie catalog instead of raw Parquet.
"""
from __future__ import annotations

import argparse

# Copies of idp.features.credit_features.CREDIT_FEATURES / fraud_features.FRAUD_FEATURES
# (kept in sync by tests/test_spark_parity.py).
CREDIT_FEATURES = [
    "account_tenure_days", "num_products", "has_device_agreement", "is_company",
    "recharge_count", "recharge_success_rate", "recharge_avg_amount", "recharge_max",
    "recharge_total", "recharge_total_30d", "recharge_total_90d", "failed_recharge_count",
    "days_since_last_recharge", "share_credit_card",
    "usage_charged_amount", "usage_charged_amount_30d", "voice_minutes", "data_bytes",
    "arpu_monthly", "spend_to_recharge_ratio",
    "fee_total", "fee_recurring_total", "fee_overage_count", "fee_charge_count", "fee_total_30d",
]
FRAUD_FEATURES = [
    "calls_per_active_day", "originating_ratio", "unique_called_numbers",
    "voice_call_count", "data_bytes", "roaming_share", "roaming_event_count",
    "usage_charged_amount", "recharge_count", "failed_recharge_count",
    "recharge_success_rate", "recharge_max", "share_credit_card",
    "account_tenure_days", "recharge_amount_zscore",
]
KEY = ["tenant_id", "service_id"]
DAY_US = 86_400 * 1_000_000


def configure(spark) -> None:
    """UTC day arithmetic + per-tenant (dynamic) partition overwrite, as in silver_job."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")


def _ref_us(df, col: str):
    """Max timestamp of ``col`` over the whole table, in epoch micros (pandas
    ``_reference_time``: one reference per table, across tenants)."""
    from pyspark.sql import functions as F
    return df.agg(F.max(F.unix_micros(F.col(col)))).collect()[0][0]


def _ok(col_value, cond):
    from pyspark.sql import functions as F
    return F.when(cond, col_value)


def _cnt(cond):
    from pyspark.sql import functions as F
    return F.sum(F.when(cond, 1).otherwise(0))


def recharge_aggregates(recharge, windows):
    from pyspark.sql import functions as F
    ref = _ref_us(recharge, "created_at")
    ts = F.unix_micros(F.col("created_at"))
    ok = F.col("status") == "successful"
    aggs = [
        F.count(F.lit(1)).alias("recharge_count"),
        _cnt(F.col("status") == "failed").alias("failed_recharge_count"),
        (_cnt(ok) / F.count(F.lit(1))).alias("recharge_success_rate"),
        F.avg(_ok(F.col("amount"), ok)).alias("recharge_avg_amount"),
        F.max(_ok(F.col("amount"), ok)).alias("recharge_max"),
        F.sum(_ok(F.col("amount"), ok)).alias("recharge_total"),
        F.floor((F.lit(ref) - F.max(ts)) / F.lit(DAY_US)).alias("days_since_last_recharge"),
        (_cnt(F.col("payment_method") == "credit_card") / F.count(F.lit(1))).alias("share_credit_card"),
    ]
    for w in windows:
        inwin = ok & (ts >= F.lit(ref - w * DAY_US))
        aggs += [F.sum(_ok(F.col("amount"), inwin)).alias(f"recharge_total_{w}d"),
                 _cnt(inwin).alias(f"recharge_count_{w}d")]
    return recharge.groupBy(*KEY).agg(*aggs)


def usage_aggregates(usage, windows):
    from pyspark.sql import functions as F
    ref = _ref_us(usage, "started_at")
    ts = F.unix_micros(F.col("started_at"))
    voice = F.col("event_type") == "voice"
    data = F.col("event_type") == "data"
    roaming = F.col("is_roaming") == F.lit(True)
    voice_calls = _cnt(voice)
    aggs = [
        F.count(F.lit(1)).alias("usage_event_count"),
        F.sum("gross_amount").alias("usage_charged_amount"),
        (F.sum(_ok(F.col("qty"), voice)) / F.lit(60.0)).alias("voice_minutes"),
        voice_calls.alias("voice_call_count"),
        F.sum(_ok(F.col("qty"), data)).alias("data_bytes"),
        _cnt(roaming).alias("roaming_event_count"),
        (_cnt(roaming) / F.count(F.lit(1))).alias("roaming_share"),
        (_cnt(voice & (F.col("traffic_case") == "originating")) / voice_calls).alias("originating_ratio"),
        F.countDistinct(_ok(F.col("other_party_number"), voice)).alias("unique_called_numbers"),
        # calls per active (UTC) day
        (voice_calls / F.countDistinct(_ok(F.to_date("started_at"), voice))).alias("calls_per_active_day"),
    ]
    for w in windows:
        inwin = ts >= F.lit(ref - w * DAY_US)
        aggs += [F.sum(_ok(F.col("gross_amount"), inwin)).alias(f"usage_charged_amount_{w}d"),
                 F.sum(_ok(F.col("qty"), inwin & data)).alias(f"data_bytes_{w}d")]
    return usage.groupBy(*KEY).agg(*aggs)


def fee_aggregates(fees, dim, windows):
    """Fees attributed to a subscriber via agreement_id -> service_id (ADR-0023)."""
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
        F.sum(_ok(F.col("amount"), ct == "recurring")).alias("fee_recurring_total"),
        _cnt(ct == "overage").alias("fee_overage_count"),
        F.sum(_ok(F.col("amount"), ct == "discount")).alias("fee_discount_total"),
    ]
    for w in windows:
        aggs.append(F.sum(_ok(F.col("amount"), ts >= F.lit(ref - w * DAY_US))).alias(f"fee_total_{w}d"))
    return df.groupBy(*KEY).agg(*aggs)


def _merge(dim, *aggregates):
    """dim LEFT JOIN each aggregate, missing numbers -> 0 (pandas merge_features)."""
    out = dim
    for agg in aggregates:
        if agg is not None:
            out = out.join(agg, KEY, "left")
    return out.na.fill(0)


def _select(df, features):
    from pyspark.sql import functions as F
    cols = [F.col(c) if c in df.columns else F.lit(0.0).alias(c) for c in features]
    return df.select(*KEY, *cols)


def build_credit_features(dim, recharge, usage, fees, windows):
    from pyspark.sql import functions as F
    feats = _merge(dim, recharge_aggregates(recharge, windows), usage_aggregates(usage, windows),
                   fee_aggregates(fees, dim, windows))
    feats = (feats
             .withColumn("has_device_agreement", F.coalesce(F.col("has_device_agreement").cast("int"), F.lit(0)))
             .withColumn("is_company", F.coalesce(F.col("is_company").cast("int"), F.lit(0))))
    months = F.greatest(F.col("account_tenure_days"), F.lit(30)) / F.lit(30.0)
    feats = feats.withColumn("arpu_monthly", F.bround(F.col("usage_charged_amount") / months, 4))
    feats = feats.withColumn(
        "spend_to_recharge_ratio",
        F.when(F.col("recharge_total") == 0, F.lit(0.0))
         .otherwise(F.bround(F.col("usage_charged_amount") / F.col("recharge_total"), 4)),
    )
    return _select(feats, CREDIT_FEATURES)


def build_fraud_features(dim, recharge, usage, windows):
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    feats = _merge(dim, recharge_aggregates(recharge, windows), usage_aggregates(usage, windows))
    # per-tenant population z-score of the max recharge (card-fraud signal)
    w = Window.partitionBy("tenant_id")
    std = F.stddev_pop("recharge_max").over(w)
    feats = feats.withColumn(
        "recharge_amount_zscore",
        F.when(std.isNull() | (std == 0), F.lit(0.0))
         .otherwise((F.col("recharge_max") - F.avg("recharge_max").over(w)) / std),
    )
    return _select(feats, FRAUD_FEATURES)


def build_gold(spark, prefix: str = "idp", *, silver: str | None = None,
               gold: str | None = None, windows=(7, 30, 90)) -> None:
    """Silver -> Gold feature tables (parity with ``idp.pipeline.to_gold``)."""
    configure(spark)
    silver = silver or f"s3a://{prefix}-silver"
    gold = gold or f"s3a://{prefix}-gold"
    windows = tuple(int(w) for w in windows)

    dim = spark.read.parquet(f"{silver}/dim_subscriber")
    usage = spark.read.parquet(f"{silver}/fact_usage")
    recharge = spark.read.parquet(f"{silver}/fact_recharge")
    try:
        fees = spark.read.parquet(f"{silver}/fact_fee_charge")
    except Exception:  # noqa: BLE001 - optional (older Silver has no fee fact)
        fees = None

    credit = build_credit_features(dim, recharge, usage, fees, windows)
    credit.write.mode("overwrite").partitionBy("tenant_id").parquet(f"{gold}/credit_features")
    fraud = build_fraud_features(dim, recharge, usage, windows)
    fraud.write.mode("overwrite").partitionBy("tenant_id").parquet(f"{gold}/fraud_features")


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default="idp", help="Bucket prefix (<prefix>-silver / -gold)")
    parser.add_argument("--silver", default=None, help="Silver base URI (default s3a://<prefix>-silver)")
    parser.add_argument("--gold", default=None, help="Gold base URI (default s3a://<prefix>-gold)")
    parser.add_argument("--windows", default="7,30,90",
                        help="Lookback windows in days (config/platform.yaml features.windows)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("idp-gold").getOrCreate()
    try:
        build_gold(spark, args.prefix, silver=args.silver, gold=args.gold,
                   windows=[w for w in args.windows.split(",") if w])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
