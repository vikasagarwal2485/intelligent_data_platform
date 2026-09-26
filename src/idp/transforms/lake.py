"""Lake layer locations and partitioned table I/O for Spark (REQ-FND-04).

A layer resolves to the same data for Spark and for the pandas/ML readers in
``idp.common.storage``: ``s3a://<prefix>-<layer>`` vs ``s3://<prefix>-<layer>``
when S3 is enabled, otherwise the same local directory.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from idp.common.config import PlatformConfig

TENANT_PARTITION = "tenant_id"


def layer_uri(cfg: PlatformConfig, layer: str) -> str:
    """Spark location of a lake layer ('bronze'|'silver'|'gold')."""
    s3 = cfg.s3
    if s3.get("enabled"):
        return f"s3a://{s3['bucket_prefix']}-{layer}"
    return str(Path(getattr(cfg.storage, layer)).resolve())


def table_uri(cfg: PlatformConfig, layer: str, table: str) -> str:
    return f"{layer_uri(cfg, layer)}/{table}"


def read_table(spark: Any, cfg: PlatformConfig, layer: str, table: str) -> Any | None:
    """Read a lake table, or None when it has not been written."""
    from pyspark.errors import AnalysisException

    try:
        return spark.read.parquet(table_uri(cfg, layer, table))
    except AnalysisException as e:
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e) \
                or "UNABLE_TO_INFER_SCHEMA" in str(e):
            return None
        raise


def write_table(df: Any, cfg: PlatformConfig, layer: str, table: str) -> None:
    """Write a tenant-partitioned table, replacing only the tenants present in
    ``df`` (dynamic partition overwrite; REQ-SLV-06 / REQ-GLD-04)."""
    df.write.mode("overwrite").partitionBy(TENANT_PARTITION).parquet(table_uri(cfg, layer, table))
