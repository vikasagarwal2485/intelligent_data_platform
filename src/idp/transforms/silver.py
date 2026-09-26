"""Silver curation: Bronze -> canonical Silver tables, on Spark (plan section 3.3).

Pure DataFrame functions (no I/O) building the four canonical tables:

* ``dim_subscriber``  - party + account + agreement + product count, one row per MSISDN
* ``fact_usage``      - unified voice + data events
* ``fact_recharge``   - recharge events
* ``fact_fee_charge`` - product fee charges, enriched from products (REQ-SLV-05)

Curation steps (plan 3.3): ``tenant_id`` carried on every table, id
canonicalization (strip ``UUID(...)``), money normalization (net + tax -> gross),
per-tenant salted PII tokenization (REQ-FND-03), and de-duplication on natural keys.
"""
from __future__ import annotations

from typing import Any

from idp.transforms.tokens import salt_col, strip_uuid, token

DIM_COLUMNS = [
    "tenant_id", "service_id", "imsi", "party_id", "account_id", "agreement_id",
    "account_type", "account_segment", "agreement_type", "business_line",
    "account_tenure_days", "date_of_birth", "language", "is_company",
    "consent_marketing", "consent_analytics", "num_products", "has_device_agreement",
]
REQUIRED_ENTITIES = ("parties", "accounts", "agreements")


def build_dim_subscriber(parties: Any, accounts: Any, agreements: Any, products: Any | None,
                         salts: dict[str, str]) -> Any:
    """One row per subscriber (MSISDN) joining party/account/agreement/products."""
    from pyspark.sql import functions as F

    ag = (
        agreements
        .withColumn("account_id", strip_uuid(F.col("account_id")))
        .withColumn("agreement_id", strip_uuid(F.col("id")))
        .select("tenant_id", "agreement_id", "agreement_type", "business_line",
                F.col("valid_from").alias("agreement_valid_from"), "msisdn", "imsi", "account_id")
    )
    ac = (
        accounts
        .withColumn("account_id", strip_uuid(F.col("id")))
        .withColumn("party_id", strip_uuid(F.col("party_id")))
        .select("tenant_id", "account_id", "account_type", "account_segment",
                F.col("valid_from").alias("account_valid_from"), "party_id", "is_company")
    )
    pa = (
        parties
        .withColumn("party_id", strip_uuid(F.col("id")))
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
    # Tenure in whole elapsed days vs the latest account_valid_from in the batch
    # (floored like a timedelta's days, not calendar-day boundaries).
    ref = dim.agg(F.max(F.unix_seconds("account_valid_from"))).collect()[0][0]
    dim = dim.withColumn(
        "account_tenure_days",
        F.when(F.col("account_valid_from").isNull(), F.lit(0))
         .otherwise(F.floor((F.lit(ref) - F.unix_seconds("account_valid_from")) / F.lit(86400)))
         .cast("long"),
    )
    sc = salt_col(salts)
    dim = (dim.withColumnRenamed("msisdn", "service_id")
              .withColumn("service_id", token(F.col("service_id"), sc))
              .withColumn("imsi", token(F.col("imsi"), sc)))
    return (dim.select([c for c in DIM_COLUMNS if c in dim.columns])
               .dropDuplicates(["tenant_id", "service_id"]))


def build_fact_usage(voice: Any | None, data: Any | None, salts: dict[str, str]) -> Any | None:
    """Unified voice + data usage fact with normalized money and tokenized PII."""
    from pyspark.sql import functions as F

    parts = [df for df in (voice, data) if df is not None]
    if not parts:
        return None
    usage = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1], allowMissingColumns=True)
    usage = (usage
             .withColumn("net_amount", F.coalesce(F.col("net_amount"), F.lit(0.0)))
             .withColumn("tax_amount", F.coalesce(F.col("tax_amount"), F.lit(0.0)))
             .withColumn("gross_amount", F.bround(F.col("net_amount") + F.col("tax_amount"), 5))
             .withColumn("started_at", F.to_timestamp("started_at"))
             .dropDuplicates(["tenant_id", "id"]))
    sc = salt_col(salts)
    for col in ("service_id", "imei", "other_party_number"):
        usage = usage.withColumn(col, token(F.col(col), sc))
    return usage


def build_fact_recharge(recharge: Any | None, salts: dict[str, str]) -> Any | None:
    """Recharge fact with tokenized PII and UTC timestamps."""
    from pyspark.sql import functions as F

    if recharge is None:
        return None
    rech = (recharge
            .withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
            .dropDuplicates(["tenant_id", "id"]))
    return rech.withColumn("service_id", token(F.col("service_id"), salt_col(salts)))


def build_fact_fee_charge(fee_charges: Any | None, products: Any | None) -> Any | None:
    """Product fee-charge fact (recurring/one_time/overage/discount), REQ-SLV-05.

    The charge references a product only; ``product_name``, ``category`` and the
    owning ``agreement_id`` are re-derived from ``products`` (the product owns
    them). No PII, so no tokenization.
    """
    from pyspark.sql import functions as F

    if fee_charges is None:
        return None
    fees = (fee_charges
            .withColumn("product_id", strip_uuid(F.col("product_id")))
            .withColumn("charged_at", F.to_timestamp("charged_at"))
            .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
            .dropDuplicates(["tenant_id", "id"]))
    if products is None:
        return fees
    cols = [F.col("tenant_id"), strip_uuid(F.col("id")).alias("product_id"),
            F.col("name").alias("product_name")]
    if "category" in products.columns:
        cols.append(F.col("category"))
    if "agreement_id" in products.columns:
        cols.append(strip_uuid(F.col("agreement_id")).alias("agreement_id"))
    fees = fees.join(products.select(*cols), ["tenant_id", "product_id"], "left")
    # column order: the charge's own columns, then the product attributes
    fee_cols = [c for c in fee_charges.columns if c != "tenant_id"]
    prod_cols = [c for c in ("product_name", "category", "agreement_id") if c in fees.columns]
    return fees.select("tenant_id", *fee_cols, *prod_cols)


def curate(bronze: dict[str, Any], salts: dict[str, str]) -> dict[str, Any]:
    """All Silver tables from the Bronze entity DataFrames (absent entities -> None).

    Raises when a required entity (parties/accounts/agreements) is missing.
    """
    missing = [e for e in REQUIRED_ENTITIES if bronze.get(e) is None]
    if missing:
        raise RuntimeError(
            f"Bronze is missing entity tables {missing}. Populate Bronze first "
            "('idp produce' then 'idp consume')."
        )
    tables = {
        "dim_subscriber": build_dim_subscriber(bronze["parties"], bronze["accounts"],
                                               bronze["agreements"], bronze.get("products"), salts),
        "fact_usage": build_fact_usage(bronze.get("voice_events"), bronze.get("data_events"), salts),
        "fact_recharge": build_fact_recharge(bronze.get("recharge_events"), salts),
        "fact_fee_charge": build_fact_fee_charge(bronze.get("fee_charges"), bronze.get("products")),
    }
    return {name: df for name, df in tables.items() if df is not None}
