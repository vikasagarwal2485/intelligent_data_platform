"""Multi-tenancy & data-isolation helpers (plan section 1.5).

In production, isolation is enforced physically (per-tenant Kafka ACLs, S3
prefixes, Iceberg namespaces, Trino+OPA row policies). In this prototype the
same contract is enforced in code: every dataset carries a ``tenant_id`` column
and all reads must go through :func:`scope_to_tenant`, which guarantees a caller
can never see another tenant's rows.
"""
from __future__ import annotations

import pandas as pd

TENANT_COLUMN = "tenant_id"


class TenantIsolationError(RuntimeError):
    """Raised when an operation would cross a tenant boundary."""


def require_tenant_column(df: pd.DataFrame) -> None:
    if TENANT_COLUMN not in df.columns:
        raise TenantIsolationError(
            f"DataFrame is missing the mandatory {TENANT_COLUMN!r} column; "
            "every record must be tenant-stamped at ingestion."
        )


def stamp_tenant(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """Stamp/normalize the tenant_id column (idempotent)."""
    out = df.copy()
    out[TENANT_COLUMN] = tenant_id
    return out


def scope_to_tenant(df: pd.DataFrame, tenant_id: str) -> pd.DataFrame:
    """Return only the rows belonging to ``tenant_id``.

    This is the single gateway through which tenant data is read. It enforces
    that the data is tenant-stamped and returns an isolated copy.
    """
    require_tenant_column(df)
    scoped = df[df[TENANT_COLUMN] == tenant_id].copy()
    return scoped.reset_index(drop=True)


def assert_single_tenant(df: pd.DataFrame, tenant_id: str | None = None) -> None:
    """Validate that a frame contains exactly one tenant (optionally a specific one)."""
    require_tenant_column(df)
    tenants = set(df[TENANT_COLUMN].unique())
    if len(tenants) > 1:
        raise TenantIsolationError(f"Frame mixes multiple tenants: {sorted(tenants)}")
    if tenant_id is not None and tenants and tenant_id not in tenants:
        raise TenantIsolationError(
            f"Frame tenant {tenants} does not match expected {tenant_id!r}"
        )
