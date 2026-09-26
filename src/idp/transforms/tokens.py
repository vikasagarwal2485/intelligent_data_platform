"""PII tokenization and id canonicalization as Spark column expressions.

Token scheme is identical to ``idp.common.pii.tokenize_value`` (REQ-FND-03):
``tok_`` + first 24 hex chars of ``sha256(f"{salt}:{value}")`` with a per-tenant
salt. Spark's ``sha2(..., 256)`` over the same UTF-8 string is byte-identical to
``hashlib.sha256``, so tokens written by the Silver transform join the labels
tokenized in Python. Null in -> null out.
"""
from __future__ import annotations

from itertools import chain
from typing import Any

UUID_PATTERN = r"^UUID\((.*)\)$"


def strip_uuid(col: Any) -> Any:
    """``UUID(123)`` -> ``123``; everything else passes through
    (``idp.curation.jsonapi.strip_uuid_wrapper``)."""
    from pyspark.sql import functions as F
    return F.regexp_replace(col, UUID_PATTERN, "$1")


def salt_col(salts: dict[str, str]) -> Any:
    """Per-row tenant salt from ``tenant_id`` (fallback ``default-salt-<tenant>``)."""
    from pyspark.sql import functions as F
    pairs = list(chain.from_iterable((F.lit(k), F.lit(v)) for k, v in salts.items()))
    tenant = F.col("tenant_id")
    fallback = F.concat(F.lit("default-salt-"), tenant)
    if not pairs:
        return fallback
    return F.coalesce(F.create_map(*pairs)[tenant], fallback)


def token(col: Any, salt: Any) -> Any:
    """Salted token of ``col`` using the per-row ``salt`` column."""
    from pyspark.sql import functions as F
    digest = F.sha2(F.concat(salt, F.lit(":"), col.cast("string")), 256)
    return F.concat(F.lit("tok_"), F.substring(digest, 1, 24))
