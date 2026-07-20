"""Silver curation: Bronze -> canonical Iceberg-style tables (plan section 3.3).

Builds the canonical Silver tables from raw Bronze frames:

* ``dim_subscriber`` - party + account + agreement + product, one row per MSISDN
* ``fact_usage``     - unified voice + data events
* ``fact_recharge``  - recharge events

Curation steps applied (plan 3.3): tenant carried as leading column, id
canonicalization (strip ``UUID(...)``), money normalization (net + tax ->
gross), GDPR PII tokenization with a per-tenant salt, deduplication.
"""
from __future__ import annotations

import pandas as pd

from idp.common import pii
from idp.common.tenancy import TENANT_COLUMN, require_tenant_column
from idp.curation.jsonapi import strip_uuid_wrapper


def _salt_for(tenant_id: str, salts: dict[str, str]) -> str:
    return salts.get(tenant_id, f"default-salt-{tenant_id}")


def _tokenize_per_tenant(df: pd.DataFrame, salts: dict[str, str], columns) -> pd.DataFrame:
    """Tokenize PII columns using each row's tenant salt."""
    if df.empty:
        return df
    parts = []
    for tenant_id, part in df.groupby(TENANT_COLUMN):
        parts.append(pii.tokenize_columns(part, _salt_for(str(tenant_id), salts), columns))
    return pd.concat(parts, ignore_index=True)


def build_subscriber_dim(frames: dict[str, pd.DataFrame], salts: dict[str, str]) -> pd.DataFrame:
    """One row per subscriber (MSISDN) joining party/account/agreement/products."""
    parties = frames["parties"].copy()
    accounts = frames["accounts"].copy()
    agreements = frames["agreements"].copy()
    products = frames.get("products", pd.DataFrame())

    for col in ("account_id", "party_id", "agreement_id"):
        for df in (parties, accounts, agreements):
            if col in df.columns:
                df[col] = df[col].map(strip_uuid_wrapper)

    prod_counts = (
        products.groupby(["tenant_id", "agreement_id"]).size().rename("num_products").reset_index()
        if not products.empty else pd.DataFrame(columns=["tenant_id", "agreement_id", "num_products"])
    )

    dim = (
        agreements.rename(columns={"id": "agreement_id", "valid_from": "agreement_valid_from"})
        .merge(
            accounts.rename(columns={"id": "account_id", "valid_from": "account_valid_from"}),
            on=["tenant_id", "account_id"], how="left", suffixes=("", "_acct"),
        )
        .merge(
            parties.rename(columns={"id": "party_id"}),
            on=["tenant_id", "party_id"], how="left", suffixes=("", "_party"),
        )
        .merge(prod_counts, on=["tenant_id", "agreement_id"], how="left")
    )
    dim["num_products"] = dim["num_products"].fillna(0).astype(int)
    dim["has_device_agreement"] = dim["agreement_type"].eq("device")

    # account tenure in days as of the latest account_valid_from reference
    dim["account_valid_from"] = pd.to_datetime(dim["account_valid_from"], utc=True, errors="coerce")
    ref = dim["account_valid_from"].max()
    dim["account_tenure_days"] = (ref - dim["account_valid_from"]).dt.days.fillna(0).astype(int)

    keep = [
        "tenant_id", "msisdn", "imsi", "party_id", "account_id", "agreement_id",
        "account_type", "account_segment", "agreement_type", "business_line", "account_tenure_days",
        "date_of_birth", "language", "is_company", "consent_marketing", "consent_analytics",
        "num_products", "has_device_agreement",
    ]
    dim = dim[[c for c in keep if c in dim.columns]].copy()
    dim = dim.rename(columns={"msisdn": "service_id"})
    dim = _tokenize_per_tenant(dim, salts, columns=("service_id", "imsi"))
    require_tenant_column(dim)
    return dim.drop_duplicates(subset=["tenant_id", "service_id"]).reset_index(drop=True)


def build_fact_usage(frames: dict[str, pd.DataFrame], salts: dict[str, str]) -> pd.DataFrame:
    """Unified voice + data usage fact with normalized money and tokenized PII."""
    voice = frames.get("voice_events", pd.DataFrame())
    data = frames.get("data_events", pd.DataFrame())
    usage = pd.concat([df for df in (voice, data) if not df.empty], ignore_index=True)
    if usage.empty:
        return usage
    usage["net_amount"] = usage.get("net_amount", 0.0).fillna(0.0)
    usage["tax_amount"] = usage.get("tax_amount", 0.0).fillna(0.0)
    usage["gross_amount"] = (usage["net_amount"] + usage["tax_amount"]).round(5)
    usage["started_at"] = pd.to_datetime(usage["started_at"], utc=True, errors="coerce")
    usage = usage.drop_duplicates(subset=["tenant_id", "id"])
    usage = _tokenize_per_tenant(usage, salts, columns=("service_id", "imei", "other_party_number"))
    require_tenant_column(usage)
    return usage.reset_index(drop=True)


def build_fact_recharge(frames: dict[str, pd.DataFrame], salts: dict[str, str]) -> pd.DataFrame:
    """Recharge fact with tokenized PII and normalized timestamps."""
    rech = frames.get("recharge_events", pd.DataFrame())
    if rech.empty:
        return rech
    rech = rech.copy()
    rech["created_at"] = pd.to_datetime(rech["created_at"], utc=True, errors="coerce")
    rech["amount"] = rech["amount"].fillna(0.0)
    rech = rech.drop_duplicates(subset=["tenant_id", "id"])
    rech = _tokenize_per_tenant(rech, salts, columns=("service_id",))
    require_tenant_column(rech)
    return rech.reset_index(drop=True)


def build_fact_fee_charge(frames: dict[str, pd.DataFrame], salts: dict[str, str]) -> pd.DataFrame:
    """Product fee-charge fact (recurring/one_time/overage/discount).

    The raw charge is normalized: it references a PRODUCT only (``product_id``).
    Silver re-derives ``product_name``, ``category`` and ``agreement_id`` by
    joining the products table (the product owns those attributes), so analytics
    can do product/category breakdowns and features can attribute fees to a
    subscriber (agreement -> service). The charge itself carries no PII, so no
    tokenization is needed here.
    """
    fees = frames.get("fee_charges", pd.DataFrame())
    if fees.empty:
        return fees
    fees = fees.copy()
    fees["product_id"] = fees["product_id"].map(strip_uuid_wrapper)
    fees["charged_at"] = pd.to_datetime(fees["charged_at"], utc=True, errors="coerce")
    fees["amount"] = fees["amount"].fillna(0.0)
    fees = fees.drop_duplicates(subset=["tenant_id", "id"])

    products = frames.get("products", pd.DataFrame())
    if not products.empty:
        prod = products.copy()
        prod["id"] = prod["id"].map(strip_uuid_wrapper)
        if "agreement_id" in prod.columns:
            prod["agreement_id"] = prod["agreement_id"].map(strip_uuid_wrapper)
        cols = (["tenant_id", "id", "name"]
                + (["category"] if "category" in prod.columns else [])
                + (["agreement_id"] if "agreement_id" in prod.columns else []))
        prod = prod[cols].rename(columns={"id": "product_id", "name": "product_name"})
        fees = fees.merge(prod, on=["tenant_id", "product_id"], how="left")
    require_tenant_column(fees)
    return fees.reset_index(drop=True)


def curate(frames: dict[str, pd.DataFrame], salts: dict[str, str]) -> dict[str, pd.DataFrame]:
    """Build all Silver tables from Bronze frames."""
    return {
        "dim_subscriber": build_subscriber_dim(frames, salts),
        "fact_usage": build_fact_usage(frames, salts),
        "fact_recharge": build_fact_recharge(frames, salts),
        "fact_fee_charge": build_fact_fee_charge(frames, salts),
    }
