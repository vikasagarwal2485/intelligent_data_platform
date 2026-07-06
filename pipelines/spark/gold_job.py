"""Production Gold stage as a PySpark job (Silver -> Gold on S3/MinIO).

This is the distributed counterpart of the on-demand `idp gold` stage: it reads
the Silver tables from object storage, computes per-subscriber feature tables,
and writes them to the Gold bucket — the same layering, on Spark, so it scales to
billions of rows (plan section 4.3).

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


def build_gold(spark, prefix: str = "idp") -> None:
    from pyspark.sql import functions as F

    silver = f"s3a://{prefix}-silver"
    gold = f"s3a://{prefix}-gold"

    dim = spark.read.parquet(f"{silver}/dim_subscriber")
    usage = spark.read.parquet(f"{silver}/fact_usage")
    recharge = spark.read.parquet(f"{silver}/fact_recharge")

    key = ["tenant_id", "service_id"]

    # --- credit features (recharge ability/reliability + spend) --------------
    rech = (
        recharge.groupBy(*key).agg(
            F.count("*").alias("recharge_count"),
            F.avg(F.when(F.col("status") == "successful", F.col("amount"))).alias("recharge_avg_amount"),
            F.sum(F.when(F.col("status") == "successful", F.col("amount"))).alias("recharge_total"),
            (F.sum(F.when(F.col("status") == "successful", 1).otherwise(0)) / F.count("*")).alias("recharge_success_rate"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_recharge_count"),
        )
    )
    spend = usage.groupBy(*key).agg(
        F.sum("gross_amount").alias("usage_charged_amount"),
        (F.sum(F.when(F.col("event_type") == "voice", F.col("qty")).otherwise(0)) / F.lit(60.0)).alias("voice_minutes"),
    )

    credit = (
        dim.select(*key, "account_tenure_days", "num_products")
        .join(rech, key, "left").join(spend, key, "left").na.fill(0)
    )
    credit.write.mode("overwrite").partitionBy("tenant_id").parquet(f"{gold}/credit_features")

    # --- fraud features (velocity / roaming / recharge anomalies) -----------
    voice = usage.filter(F.col("event_type") == "voice")
    fraud = (
        dim.select(*key, "account_tenure_days")
        .join(
            voice.groupBy(*key).agg(
                F.count("*").alias("voice_call_count"),
                (F.sum(F.when(F.col("traffic_case") == "originating", 1).otherwise(0)) / F.count("*")).alias("originating_ratio"),
                F.countDistinct("other_party_number").alias("unique_called_numbers"),
            ), key, "left")
        .join(
            usage.groupBy(*key).agg(
                (F.sum(F.when(F.col("is_roaming"), 1).otherwise(0)) / F.count("*")).alias("roaming_share"),
            ), key, "left")
        .na.fill(0)
    )
    fraud.write.mode("overwrite").partitionBy("tenant_id").parquet(f"{gold}/fraud_features")


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default="idp", help="Bucket prefix (<prefix>-silver / -gold)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("idp-gold").getOrCreate()
    try:
        build_gold(spark, args.prefix)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
