"""Production Silver stage as a PySpark job (Bronze -> canonical Silver on S3/MinIO).

Distributed counterpart of the on-demand `idp silver` stage (src/idp/curation/silver.py::curate).
Reads the Bronze entity tables and builds the canonical Silver tables:

  * dim_subscriber  — party ⋈ account ⋈ agreement ⋈ product-count, one row per MSISDN
  * fact_usage      — unified voice + data events (net+tax -> gross)
  * fact_recharge   — recharge events
  * fact_fee_charge — product fee charges, enriched from products

CRITICAL — token parity: PII is tokenized with the SAME scheme as the pandas
path (idp.common.pii.tokenize_value): ``tok_ + sha256(f"{salt}:{value}")[:24]``
with a per-tenant salt. Spark's ``sha2(..., 256)`` is byte-identical to Python's
``hashlib.sha256`` for the same UTF-8 string, so the ``service_id`` token — the
downstream join key into Gold and the serving layer — is identical whether Silver
ran on pandas or Spark. Pass the salts with ``--salts tenant_a=...,tenant_b=...``.

Submit (against MinIO):
  spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio.idp-platform.svc:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    pipelines/spark/silver_job.py --prefix idp --salts tenant_a=salt-a,tenant_b=salt-b

On the target platform this reads/writes Iceberg tables via the Nessie catalog
instead of raw Parquet.
"""
from __future__ import annotations

import argparse
from itertools import chain


def _strip_uuid(col):
    """`UUID(123)` -> `123`; pass everything else through (mirrors strip_uuid_wrapper)."""
    from pyspark.sql import functions as F
    return F.regexp_replace(col, r"^UUID\((.*)\)$", "$1")


def _salt_col(salts):
    """Per-row salt from tenant_id, matching pii._salt_for's default fallback."""
    from pyspark.sql import functions as F
    pairs = list(chain.from_iterable((F.lit(k), F.lit(v)) for k, v in salts.items()))
    salt_map = F.create_map(*pairs) if pairs else None
    tenant = F.col("tenant_id")
    fallback = F.concat(F.lit("default-salt-"), tenant)
    return F.coalesce(salt_map[tenant], fallback) if salt_map is not None else fallback


def _token(col, salt_col):
    """tok_ + sha256(f"{salt}:{value}")[:24]; null in -> null out (== pandas None)."""
    from pyspark.sql import functions as F
    digest = F.sha2(F.concat(salt_col, F.lit(":"), col.cast("string")), 256)
    return F.concat(F.lit("tok_"), F.substring(digest, 1, 24))


def configure(spark) -> None:
    """Session settings both Spark stages rely on for parity with the pandas path.

    * UTC session time zone, so timestamp parsing and day arithmetic match pandas'
      ``utc=True`` handling.
    * Dynamic partition overwrite: ``mode("overwrite").partitionBy("tenant_id")``
      replaces only the tenants present in the output (REQ-ORC-04).
    * Parquet timestamps as UTC-adjusted INT64 micros, which pandas reads back as
      tz-aware UTC (the same logical type the pandas path writes).
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")


def _read_optional(spark, uri):
    """Read a Parquet table, or None when it does not exist (optional entities)."""
    try:
        return spark.read.parquet(uri)
    except Exception:  # noqa: BLE001 - AnalysisException: path does not exist
        return None


def _write(df, uri: str) -> None:
    df.write.mode("overwrite").partitionBy("tenant_id").parquet(uri)


def build_silver(spark, prefix: str = "idp", salts: dict | None = None, *,
                 bronze: str | None = None, silver: str | None = None) -> None:
    """Bronze -> the four canonical Silver tables (parity with ``idp.curation.silver``)."""
    from pyspark.sql import functions as F

    configure(spark)
    salts = salts or {}
    bronze = bronze or f"s3a://{prefix}-bronze"
    silver = silver or f"s3a://{prefix}-silver"
    sc = _salt_col(salts)

    parties = spark.read.parquet(f"{bronze}/parties")
    accounts = spark.read.parquet(f"{bronze}/accounts")
    agreements = spark.read.parquet(f"{bronze}/agreements")
    voice = spark.read.parquet(f"{bronze}/voice_events")
    data = spark.read.parquet(f"{bronze}/data_events")
    recharge = spark.read.parquet(f"{bronze}/recharge_events")
    products = _read_optional(spark, f"{bronze}/products")        # optional
    fee_charges = _read_optional(spark, f"{bronze}/fee_charges")  # optional

    # --- dim_subscriber -----------------------------------------------------
    ag = (
        agreements
        .withColumn("account_id", _strip_uuid(F.col("account_id")))
        .withColumn("agreement_id", _strip_uuid(F.col("id")))
        .select("tenant_id", "agreement_id", "agreement_type", "business_line",
                F.col("valid_from").alias("agreement_valid_from"), "msisdn", "imsi", "account_id")
    )
    ac = (
        accounts
        .withColumn("account_id", _strip_uuid(F.col("id")))
        .withColumn("party_id", _strip_uuid(F.col("party_id")))
        .select("tenant_id", "account_id", "account_type", "account_segment",
                F.col("valid_from").alias("account_valid_from"), "party_id", "is_company")
    )
    pa = (
        parties
        .withColumn("party_id", _strip_uuid(F.col("id")))
        .select("tenant_id", "party_id", "date_of_birth", "language",
                "consent_marketing", "consent_analytics")
    )

    dim = ag.join(ac, ["tenant_id", "account_id"], "left").join(pa, ["tenant_id", "party_id"], "left")

    if products is not None:
        prod_counts = (products.groupBy("tenant_id", "agreement_id").count()
                       .withColumnRenamed("count", "num_products"))
        dim = dim.join(prod_counts, ["tenant_id", "agreement_id"], "left")
        dim = dim.withColumn("num_products", F.coalesce(F.col("num_products"), F.lit(0)).cast("long"))
    else:
        dim = dim.withColumn("num_products", F.lit(0).cast("long"))

    dim = dim.withColumn("has_device_agreement", F.col("agreement_type") == F.lit("device"))
    dim = dim.withColumn("account_valid_from", F.to_timestamp("account_valid_from"))
    # Tenure in whole days vs the latest account_valid_from in the batch, floored
    # like pandas' Timedelta.days (datediff would count calendar-day boundaries).
    ref = dim.agg(F.max("account_valid_from")).collect()[0][0]
    secs = F.lit(ref).cast("long") - F.col("account_valid_from").cast("long")
    dim = dim.withColumn(
        "account_tenure_days",
        F.when(F.col("account_valid_from").isNull(), F.lit(0))
         .otherwise(F.floor(secs / F.lit(86400))).cast("long"),
    )
    dim = dim.withColumnRenamed("msisdn", "service_id")
    dim = dim.withColumn("service_id", _token(F.col("service_id"), sc)) \
             .withColumn("imsi", _token(F.col("imsi"), sc))

    keep = ["tenant_id", "service_id", "imsi", "party_id", "account_id", "agreement_id",
            "account_type", "account_segment", "agreement_type", "business_line",
            "account_tenure_days", "date_of_birth", "language", "is_company",
            "consent_marketing", "consent_analytics", "num_products", "has_device_agreement"]
    dim = dim.select([c for c in keep if c in dim.columns]).dropDuplicates(["tenant_id", "service_id"])
    _write(dim, f"{silver}/dim_subscriber")

    # --- fact_usage (voice + data) ------------------------------------------
    usage = voice.unionByName(data, allowMissingColumns=True)
    usage = (usage
             .withColumn("net_amount", F.coalesce(F.col("net_amount"), F.lit(0.0)))
             .withColumn("tax_amount", F.coalesce(F.col("tax_amount"), F.lit(0.0)))
             .withColumn("gross_amount", F.bround(F.col("net_amount") + F.col("tax_amount"), 5))
             .withColumn("started_at", F.to_timestamp("started_at"))
             .dropDuplicates(["tenant_id", "id"]))
    for col in ("service_id", "imei", "other_party_number"):
        usage = usage.withColumn(col, _token(F.col(col), sc))
    _write(usage, f"{silver}/fact_usage")

    # --- fact_recharge ------------------------------------------------------
    rech = (recharge
            .withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
            .dropDuplicates(["tenant_id", "id"]))
    rech = rech.withColumn("service_id", _token(F.col("service_id"), sc))
    _write(rech, f"{silver}/fact_recharge")

    # --- fact_fee_charge (REQ-SLV-05) ---------------------------------------
    # References a product only; product_name/category/agreement_id are re-derived
    # from products (the product owns them). No PII, so no tokenization.
    if fee_charges is not None:
        fees = (fee_charges
                .withColumn("product_id", _strip_uuid(F.col("product_id")))
                .withColumn("charged_at", F.to_timestamp("charged_at"))
                .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
                .dropDuplicates(["tenant_id", "id"]))
        if products is not None:
            cols = [F.col("tenant_id"), _strip_uuid(F.col("id")).alias("product_id"),
                    F.col("name").alias("product_name")]
            if "category" in products.columns:
                cols.append(F.col("category"))
            if "agreement_id" in products.columns:
                cols.append(_strip_uuid(F.col("agreement_id")).alias("agreement_id"))
            fees = fees.join(products.select(*cols), ["tenant_id", "product_id"], "left")
            # pandas merge order: fee columns, then the product columns
            fee_cols = [c for c in fee_charges.columns if c != "tenant_id"]
            prod_cols = [c for c in ("product_name", "category", "agreement_id") if c in fees.columns]
            fees = fees.select("tenant_id", *fee_cols, *prod_cols)
        _write(fees, f"{silver}/fact_fee_charge")


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default="idp", help="Bucket prefix (<prefix>-bronze / -silver)")
    parser.add_argument("--salts", default="",
                        help="Per-tenant PII salts, e.g. tenant_a=salt-a,tenant_b=salt-b")
    parser.add_argument("--bronze", default=None, help="Bronze base URI (default s3a://<prefix>-bronze)")
    parser.add_argument("--silver", default=None, help="Silver base URI (default s3a://<prefix>-silver)")
    args = parser.parse_args()
    salts = dict(kv.split("=", 1) for kv in args.salts.split(",") if "=" in kv)

    spark = SparkSession.builder.appName("idp-silver").getOrCreate()
    try:
        build_silver(spark, args.prefix, salts, bronze=args.bronze, silver=args.silver)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
