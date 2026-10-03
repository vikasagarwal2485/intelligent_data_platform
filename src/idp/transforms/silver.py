"""Silver curation: conformed records -> canonical Silver tables, on Spark (plan 3.3).

Pure DataFrame functions (no I/O). Their inputs are the canonical records Silver's
conform step produced from the raw Bronze events (``transforms.conform``,
REQ-SLV-09): latest versions for the reference entities, one record per event for
the facts.

* ``dim_subscriber``      - agreement + customer + billing account + party + product
                            count, one row per MSISDN, with ``customer_tenure_days``
* ``dim_customer``        - one row per customer (party role), with its segment
* ``dim_billing_account`` - one row per billing account, with its paying party
* ``dim_product``         - one row per product
* ``fact_usage``          - unified voice + data events
* ``fact_recharge``       - recharge events
* ``fact_fee_charge``     - product fee charges, enriched from products (REQ-SLV-05)

Curation steps (plan 3.3): ``tenant_id`` carried on every table, id
canonicalization (strip ``UUID(...)``), money normalization (net + tax -> gross),
per-tenant salted PII tokenization (REQ-FND-03), and de-duplication on natural keys.
"""
from __future__ import annotations

from typing import Any

from idp.transforms.tokens import salt_col, strip_uuid, token

DIM_COLUMNS = [
    "tenant_id", "service_id", "imsi", "customer_id", "account_id", "agreement_id",
    "account_type", "segment", "agreement_type", "business_line",
    "customer_tenure_days", "date_of_birth", "language", "is_company",
    "consent_marketing", "consent_analytics", "num_products", "has_device_agreement",
]
DIM_CUSTOMER_COLUMNS = ["tenant_id", "customer_id", "party_id", "party_type", "role", "segment",
                        "status", "valid_from"]
DIM_BILLING_ACCOUNT_COLUMNS = ["tenant_id", "billing_account_id", "account_type", "lifecycle_status",
                               "valid_from", "payer_party_id", "is_company"]
DIM_PRODUCT_COLUMNS = ["tenant_id", "product_id", "agreement_id", "product_type", "name", "category",
                       "monthly_fee", "lifecycle_status"]
REQUIRED_ENTITIES = ("parties", "customers", "billing_accounts", "agreements")


def build_dim_subscriber(parties: Any, customers: Any, billing_accounts: Any, agreements: Any,
                         products: Any | None, salts: dict[str, str]) -> Any:
    """One row per subscriber (MSISDN): the agreement, its customer (segment, tenure
    and party attributes), its billing account, and the product count."""
    from pyspark.sql import functions as F

    ag = (
        agreements
        .withColumn("account_id", strip_uuid(F.col("account_id")))
        .withColumn("customer_id", strip_uuid(F.col("customer_id")))
        .withColumn("agreement_id", strip_uuid(F.col("id")))
        .select("tenant_id", "agreement_id", "agreement_type", "business_line",
                "msisdn", "imsi", "customer_id", "account_id")
    )
    cu = (
        customers
        .withColumn("customer_id", strip_uuid(F.col("id")))
        .withColumn("party_id", strip_uuid(F.col("party_id")))
        .select("tenant_id", "customer_id", "segment",
                F.col("valid_from").alias("customer_valid_from"), "party_id")
    )
    ac = (
        billing_accounts
        .withColumn("account_id", strip_uuid(F.col("id")))
        .select("tenant_id", "account_id", "account_type", "is_company")
    )
    pa = (
        parties
        .withColumn("party_id", strip_uuid(F.col("id")))
        .select("tenant_id", "party_id", "date_of_birth", "language",
                "consent_marketing", "consent_analytics")
    )
    dim = (ag.join(cu, ["tenant_id", "customer_id"], "left")
             .join(ac, ["tenant_id", "account_id"], "left")
             .join(pa, ["tenant_id", "party_id"], "left"))

    if products is not None:
        prod_counts = (products.groupBy("tenant_id", "agreement_id").count()
                       .withColumnRenamed("count", "num_products"))
        dim = dim.join(prod_counts, ["tenant_id", "agreement_id"], "left")
        dim = dim.withColumn("num_products", F.coalesce(F.col("num_products"), F.lit(0)).cast("long"))
    else:
        dim = dim.withColumn("num_products", F.lit(0).cast("long"))

    dim = dim.withColumn("has_device_agreement", F.col("agreement_type") == F.lit("device"))
    dim = dim.withColumn("customer_valid_from", F.to_timestamp("customer_valid_from"))
    # Customer tenure in whole elapsed days vs the latest customer valid_from in the
    # run (floored like a timedelta's days, not calendar-day boundaries); 0 if unknown.
    ref = dim.agg(F.max(F.unix_seconds("customer_valid_from"))).collect()[0][0]
    dim = dim.withColumn(
        "customer_tenure_days",
        F.when(F.col("customer_valid_from").isNull(), F.lit(0))
         .otherwise(F.floor((F.lit(ref) - F.unix_seconds("customer_valid_from")) / F.lit(86400)))
         .cast("long"),
    )
    sc = salt_col(salts)
    dim = (dim.withColumnRenamed("msisdn", "service_id")
              .withColumn("service_id", token(F.col("service_id"), sc))
              .withColumn("imsi", token(F.col("imsi"), sc)))
    return (dim.select([c for c in DIM_COLUMNS if c in dim.columns])
               .dropDuplicates(["tenant_id", "service_id"]))


def build_dim_customer(customers: Any, parties: Any | None) -> Any:
    """One row per customer, with its party's type."""
    from pyspark.sql import functions as F

    dim = (customers
           .withColumn("customer_id", strip_uuid(F.col("id")))
           .withColumn("party_id", strip_uuid(F.col("party_id")))
           .withColumn("valid_from", F.to_timestamp("valid_from")))
    if parties is not None:
        types = parties.select("tenant_id", strip_uuid(F.col("id")).alias("party_id"), "party_type")
        dim = dim.join(types, ["tenant_id", "party_id"], "left")
    else:
        dim = dim.withColumn("party_type", F.lit(None).cast("string"))
    return dim.select(*DIM_CUSTOMER_COLUMNS).dropDuplicates(["tenant_id", "customer_id"])


def build_dim_billing_account(billing_accounts: Any) -> Any:
    """One row per billing account, with its paying party."""
    from pyspark.sql import functions as F

    return (billing_accounts
            .withColumn("billing_account_id", strip_uuid(F.col("id")))
            .withColumn("payer_party_id", strip_uuid(F.col("payer_party_id")))
            .withColumn("valid_from", F.to_timestamp("valid_from"))
            .select(*DIM_BILLING_ACCOUNT_COLUMNS)
            .dropDuplicates(["tenant_id", "billing_account_id"]))


def build_dim_product(products: Any) -> Any:
    """One row per product."""
    from pyspark.sql import functions as F

    return (products
            .withColumn("product_id", strip_uuid(F.col("id")))
            .withColumn("agreement_id", strip_uuid(F.col("agreement_id")))
            .select(*DIM_PRODUCT_COLUMNS)
            .dropDuplicates(["tenant_id", "product_id"]))


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


def curate(entities: dict[str, Any], salts: dict[str, str]) -> dict[str, Any]:
    """All Silver tables from conformed canonical entity DataFrames (absent -> None).

    Raises when a required entity (parties/customers/billing_accounts/agreements)
    is missing.
    """
    missing = [e for e in REQUIRED_ENTITIES if entities.get(e) is None]
    if missing:
        raise RuntimeError(
            f"Bronze is missing entities {missing}. Populate Bronze first "
            "('idp produce' then 'idp consume')."
        )
    products = entities.get("products")
    tables = {
        "dim_subscriber": build_dim_subscriber(entities["parties"], entities["customers"],
                                               entities["billing_accounts"], entities["agreements"],
                                               products, salts),
        "dim_customer": build_dim_customer(entities["customers"], entities["parties"]),
        "dim_billing_account": build_dim_billing_account(entities["billing_accounts"]),
        "dim_product": None if products is None else build_dim_product(products),
        "fact_usage": build_fact_usage(entities.get("voice_events"), entities.get("data_events"), salts),
        "fact_recharge": build_fact_recharge(entities.get("recharge_events"), salts),
        "fact_fee_charge": build_fact_fee_charge(entities.get("fee_charges"), products),
    }
    return {name: df for name, df in tables.items() if df is not None}
