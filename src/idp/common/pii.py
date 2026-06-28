"""GDPR PII handling (plan section 1.5).

Implements deterministic, per-tenant-salted tokenization so PII (MSISDN, IMSI,
IMEI, phone, email) can be used as stable join keys in the lakehouse without
storing the raw identifier. Also provides consent-based filtering used by the
feature/ML layers (a subscriber without analytics consent is excluded from
model training/scoring).
"""
from __future__ import annotations

import hashlib

import pandas as pd

# Columns that must be tokenized before leaving the Silver layer.
PII_COLUMNS = ("msisdn", "imsi", "imei", "phone", "email", "other_party_number", "service_id")


def tokenize_value(value: str | None, salt: str) -> str | None:
    """Return a deterministic salted SHA-256 token for a single value."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    digest = hashlib.sha256(f"{salt}:{value}".encode("utf-8")).hexdigest()
    return f"tok_{digest[:24]}"


def tokenize_series(series: pd.Series, salt: str) -> pd.Series:
    return series.map(lambda v: tokenize_value(v, salt))


def tokenize_columns(df: pd.DataFrame, salt: str, columns=PII_COLUMNS) -> pd.DataFrame:
    """Tokenize all present PII columns in ``df`` using the tenant ``salt``."""
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = tokenize_series(out[col], salt)
    return out


def filter_by_consent(
    df: pd.DataFrame, purpose: str = "analytics", consent_col_prefix: str = "consent_"
) -> pd.DataFrame:
    """Keep only rows where the subscriber granted consent for ``purpose``.

    If the consent column is absent, the frame is returned unchanged (consent is
    enforced upstream at the subscriber dimension).
    """
    col = f"{consent_col_prefix}{purpose}"
    if col not in df.columns:
        return df
    return df[df[col].fillna(False).astype(bool)].reset_index(drop=True)
