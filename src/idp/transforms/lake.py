"""Lake table I/O for Spark over the Iceberg catalog (REQ-FND-04).

Tables are resolved through the same catalog the Python side uses
(``idp.common.catalog``), so Spark and pyiceberg see the same committed snapshots.
Every write is one atomic Iceberg commit: a concurrent reader sees the table
before or after it, never in between (REQ-SLV-06.3).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from idp.common import catalog, storage
from idp.common.config import PlatformConfig

TENANT_PARTITION = "tenant_id"


def layer_uri(cfg: PlatformConfig, layer: str) -> str:
    """Spark path of a layer's root (for non-table data such as stream checkpoints)."""
    s3 = cfg.s3
    if s3.get("enabled"):
        return f"s3a://{s3['bucket_prefix']}-{layer}"
    return str(Path(storage.lake_uri(cfg, layer)).resolve())


def attach(spark: Any, cfg: PlatformConfig) -> None:
    """Register ``cfg``'s lake catalog in ``spark`` (idempotent; catalogs load lazily
    by name, so one session can serve several local lakes). pyiceberg initializes it
    first: it creates the SQL catalog's tables and the layer namespaces."""
    catalog.load_catalog(cfg)
    for key, value in catalog.spark_catalog_conf(cfg).items():
        spark.conf.set(key, value)


def read_table(spark: Any, cfg: PlatformConfig, layer: str, table: str) -> Any | None:
    """A lake table as a Spark DataFrame, or None when it has never been written."""
    attach(spark, cfg)
    if not catalog.table_exists(cfg, layer, table):
        return None
    return spark.table(catalog.spark_table(cfg, layer, table))


def _partition_exprs(partition_by: Iterable[Any]) -> list[Any]:
    from pyspark.sql import functions as F
    return [F.col(p) if isinstance(p, str) else p for p in partition_by]


def ensure_table(df: Any, cfg: PlatformConfig, layer: str, table: str,
                 partition_by: Iterable[Any] = (TENANT_PARTITION,)) -> str:
    """Create the table (in its layer's location) if absent; add any new columns.

    ``partition_by`` holds column names or Spark partition transforms
    (e.g. ``F.days("started_at")``). Returns the table's Spark identifier.
    """
    attach(df.sparkSession, cfg)
    name = catalog.spark_table(cfg, layer, table)
    if not catalog.table_exists(cfg, layer, table):
        (df.limit(0).writeTo(name).using("iceberg")
           .partitionedBy(*_partition_exprs(partition_by))
           .tableProperty("location", catalog.table_location(cfg, layer, table))
           .create())
        return name
    spark = df.sparkSession
    existing = {f.name for f in spark.table(name).schema.fields}
    for field in df.schema.fields:
        if field.name not in existing:          # additive evolution only
            spark.sql(f"ALTER TABLE {name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")
    return name


def overwrite_tenants(df: Any, cfg: PlatformConfig, layer: str, table: str,
                      partition_by: Iterable[Any] = (TENANT_PARTITION,)) -> None:
    """Atomically replace the partitions present in ``df`` (the tenants it holds)."""
    name = ensure_table(df, cfg, layer, table, partition_by)
    df.writeTo(name).overwritePartitions()


def append(df: Any, cfg: PlatformConfig, layer: str, table: str,
           partition_by: Iterable[Any] = (TENANT_PARTITION,)) -> None:
    """Append ``df`` as one snapshot (e.g. a score run); nothing existing changes."""
    name = ensure_table(df, cfg, layer, table, partition_by)
    df.writeTo(name).append()


def merge_insert_new(df: Any, cfg: PlatformConfig, layer: str, table: str,
                     keys: Iterable[str], partition_by: Iterable[Any] = (TENANT_PARTITION,)) -> None:
    """Insert the rows of ``df`` whose ``keys`` are not in the table yet (one commit).

    Rows already present are left untouched, so replaying the same input never
    duplicates (REQ-SLV-06).
    """
    keys = list(keys)
    name = ensure_table(df, cfg, layer, table, partition_by)
    src = f"_merge_src_{layer}_{table}"
    df.dropDuplicates(keys).createOrReplaceTempView(src)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    cols = ", ".join(f"`{c}`" for c in df.columns)
    vals = ", ".join(f"s.`{c}`" for c in df.columns)
    df.sparkSession.sql(
        f"MERGE INTO {name} t USING {src} s ON {on} "
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
    )


def replace_all(df: Any, cfg: PlatformConfig, layer: str, table: str,
                partition_by: Iterable[Any] = (TENANT_PARTITION,)) -> None:
    """Replace the whole table's contents with ``df`` in one commit (full refresh)."""
    from pyspark.sql import functions as F
    name = ensure_table(df, cfg, layer, table, partition_by)
    df.writeTo(name).overwrite(F.lit(True))
