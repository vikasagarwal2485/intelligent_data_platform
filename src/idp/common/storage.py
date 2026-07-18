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


def _ensure_s3_bucket(uri: str, opts: dict[str, Any]) -> None:
    """Create the target S3/MinIO bucket if it doesn't exist.

    Writing to a non-existent bucket otherwise persists nothing while the write
    still "succeeds" (silent data loss) — e.g. if bucket provisioning only created
    some of idp-bronze/silver/gold/ml. This makes the write self-healing.

    IMPORTANT: use a THROWAWAY s3fs instance (``skip_instance_cache=True``). The
    write that follows (``df.to_parquet(storage_options=opts)``) builds its own
    fsspec-cached s3fs instance; if we touched that shared instance's directory
    cache here (e.g. via ``makedirs``), the subsequent *partitioned* write would
    read a stale listing and silently write zero files. Keeping this instance
    separate leaves the write's filesystem cache clean.
    """
    import s3fs

    bucket = uri.split("/", 3)[2]  # s3://<bucket>/<key...>
    fs = s3fs.S3FileSystem(
        key=opts["key"], secret=opts["secret"], client_kwargs=opts["client_kwargs"],
        skip_instance_cache=True,
    )
    try:
        if not fs.exists(bucket):
            fs.mkdir(bucket)
    except Exception:  # noqa: BLE001 - already exists / race; the write will surface real errors
        pass


def write_parquet(
    df: pd.DataFrame, uri: str, cfg: PlatformConfig, *, partition_cols: list[str] | None = None
) -> None:
    """Write a DataFrame to Parquet at ``uri`` (local or s3://)."""
    opts = storage_options(cfg)
    if opts is None and not uri.startswith("s3://"):
        Path(uri).mkdir(parents=True, exist_ok=True)
    elif opts is not None and uri.startswith("s3://"):
        _ensure_s3_bucket(uri, opts)
    df.to_parquet(uri, index=False, partition_cols=partition_cols, storage_options=opts)


def read_parquet(uri: str, cfg: PlatformConfig) -> pd.DataFrame:
    """Read Parquet from ``uri`` (local or s3://)."""
    return pd.read_parquet(uri, storage_options=storage_options(cfg))
