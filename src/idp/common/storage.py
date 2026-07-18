"""Lake storage helpers that work against local dirs or MinIO/S3.

When ``s3.enabled`` (or IDP_S3_ENDPOINT is set) the Bronze/Silver/Gold layers
are written to an S3-compatible store (MinIO) via fsspec/s3fs; otherwise to
local directories. This lets the same code run locally and in-cluster.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from idp.common.config import PlatformConfig


def storage_options(cfg: PlatformConfig) -> dict[str, Any] | None:
    """fsspec storage options for S3/MinIO, or None for local filesystem."""
    s3 = cfg.s3
    if not s3.get("enabled"):
        return None
    return {
        "key": s3["access_key"],
        "secret": s3["secret_key"],
        "client_kwargs": {"endpoint_url": s3["endpoint"]},
    }


def lake_uri(cfg: PlatformConfig, layer: str) -> str:
    """Return the base URI for a lake layer ('bronze'|'silver'|'gold').

    On S3 each layer maps to its own bucket ``<prefix>-<layer>`` (e.g.
    ``idp-bronze``, ``idp-silver``, ``idp-gold``) — matching the buckets created
    by deploy/platform/install-infra.sh.
    """
    s3 = cfg.s3
    if s3.get("enabled"):
        return f"s3://{s3['bucket_prefix']}-{layer}"
    return getattr(cfg.storage, layer)


def write_parquet(
    df: pd.DataFrame, uri: str, cfg: PlatformConfig, *, partition_cols: list[str] | None = None
) -> None:
    """Write a DataFrame to Parquet at ``uri`` (local or s3://).

    NOTE: no S3 bucket "ensure" step here. An earlier attempt to create the bucket
    first (via s3fs) shared/poisoned the fsspec filesystem cache that this write
    reuses, which made partitioned S3 writes silently produce zero files. The
    buckets are provisioned separately (install-infra.sh / `make minio-mkbuckets`),
    so this stays a plain, cache-clean ``to_parquet``.
    """
    opts = storage_options(cfg)
    if opts is None and not uri.startswith("s3://"):
        Path(uri).mkdir(parents=True, exist_ok=True)
    df.to_parquet(uri, index=False, partition_cols=partition_cols, storage_options=opts)


def read_parquet(uri: str, cfg: PlatformConfig) -> pd.DataFrame:
    """Read Parquet from ``uri`` (local or s3://)."""
    return pd.read_parquet(uri, storage_options=storage_options(cfg))
