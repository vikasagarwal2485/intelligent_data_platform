"""Production Silver stage as a PySpark job (Bronze -> canonical Silver on S3/MinIO).

Distributed counterpart of the on-demand `idp silver` stage (src/idp/curation/silver.py::curate).
Reads the Bronze entity tables and builds the canonical Silver tables:

  * dim_subscriber  — party ⋈ account ⋈ agreement ⋈ product-count, one row per MSISDN
  * fact_usage      — unified voice + data events (net+tax -> gross)
  * fact_recharge   — recharge events

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


def build_silver(spark, prefix: str = "idp", salts: dict | None = None) -> None:
    from pyspark.sql import functions as F

    salts = salts or {}
    bronze = f"s3a://{prefix}-bronze"
    silver = f"s3a://{prefix}-silver"
    sc = _salt_col(salts)

    parties = spark.read.parquet(f"{bronze}/parties")
    accounts = spark.read.parquet(f"{bronze}/accounts")
    agreements = spark.read.parquet(f"{bronze}/agreements")
    voice = spark.read.parquet(f"{bronze}/voice_events")
    data = spark.read.parquet(f"{bronze}/data_events")
    recharge = spark.read.parquet(f"{bronze}/recharge_events")
    try:
        products = spark.read.parquet(f"{bronze}/products")
    except Exception:  # noqa: BLE001 - products are optional
        products = None

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
        dim = dim.withColumn("num_products", F.coalesce(F.col("num_products"), F.lit(0)).cast("int"))
    else:
        dim = dim.withColumn("num_products", F.lit(0))

    dim = dim.withColumn("has_device_agreement", F.col("agreement_type") == F.lit("device"))
    dim = dim.withColumn("account_valid_from", F.to_timestamp("account_valid_from"))
    # tenure vs the latest account_valid_from in the batch (matches the pandas ref)
    ref = dim.agg(F.max("account_valid_from")).collect()[0][0]
    dim = dim.withColumn(
        "account_tenure_days",
        F.when(F.col("account_valid_from").isNull(), F.lit(0))
         .otherwise(F.datediff(F.lit(ref), F.col("account_valid_from"))).cast("int"),
    )
    dim = dim.withColumnRenamed("msisdn", "service_id")
    dim = dim.withColumn("service_id", _token(F.col("service_id"), sc)) \
             .withColumn("imsi", _token(F.col("imsi"), sc))

    keep = ["tenant_id", "service_id", "imsi", "party_id", "account_id", "agreement_id",
            "account_type", "account_segment", "business_line", "account_tenure_days",
            "date_of_birth", "language", "is_company", "consent_marketing", "consent_analytics",
            "num_products", "has_device_agreement"]
    dim = dim.select([c for c in keep if c in dim.columns]).dropDuplicates(["tenant_id", "service_id"])
    dim.write.mode("overwrite").partitionBy("tenant_id").parquet(f"{silver}/dim_subscriber")

    # --- fact_usage (voice + data) ------------------------------------------
    usage = voice.unionByName(data, allowMissingColumns=True)
    usage = (usage
             .withColumn("net_amount", F.coalesce(F.col("net_amount"), F.lit(0.0)))
             .withColumn("tax_amount", F.coalesce(F.col("tax_amount"), F.lit(0.0)))
             .withColumn("gross_amount", F.round(F.col("net_amount") + F.col("tax_amount"), 5))
             .withColumn("started_at", F.to_timestamp("started_at"))
             .dropDuplicates(["tenant_id", "id"]))
    for col in ("service_id", "imei", "other_party_number"):
        usage = usage.withColumn(col, _token(F.col(col), sc))
    usage.write.mode("overwrite").partitionBy("tenant_id").parquet(f"{silver}/fact_usage")

    # --- fact_recharge ------------------------------------------------------
    rech = (recharge
            .withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
            .dropDuplicates(["tenant_id", "id"]))
    rech = rech.withColumn("service_id", _token(F.col("service_id"), sc))
    rech.write.mode("overwrite").partitionBy("tenant_id").parquet(f"{silver}/fact_recharge")


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default="idp", help="Bucket prefix (<prefix>-bronze / -silver)")
    parser.add_argument("--salts", default="",
                        help="Per-tenant PII salts, e.g. tenant_a=salt-a,tenant_b=salt-b")
    args = parser.parse_args()
    salts = dict(kv.split("=", 1) for kv in args.salts.split(",") if "=" in kv)

    spark = SparkSession.builder.appName("idp-silver").getOrCreate()
    try:
        build_silver(spark, args.prefix, salts)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
