"""One-off migration of a pre-Iceberg lake (``idp migrate-lake``).

Before the Iceberg lake, Bronze was plain Parquet at ``<lake_uri(bronze)>/<entity>``
(hive-partitioned by ``tenant_id``). This appends each such entity to the Iceberg
table ``bronze.<entity>`` in ONE commit per entity, with ``_ingested_at`` set to the
migration time, and records ``idp.migrated-from`` in the table properties. A
re-run skips entities that carry the marker, so it is safe to repeat.

Silver and Gold are not migrated; they are rebuilt from the migrated Bronze with
``idp silver --full-refresh`` then ``idp gold``. The legacy Parquet is left in
place (Iceberg data lives under ``tables/``, so orphan cleanup never touches it)
until the operator removes it.
"""
from __future__ import annotations

from typing import Any

import fsspec
import pyarrow as pa
import pyarrow.dataset as ds

from idp.common import catalog, storage
from idp.common.config import PlatformConfig

MARKER = "idp.migrated-from"


def legacy_bronze_uri(cfg: PlatformConfig, entity: str) -> str:
    return f"{storage.lake_uri(cfg, 'bronze')}/{entity}"


def _legacy_dataset(cfg: PlatformConfig, uri: str) -> ds.Dataset | None:
    fs, path = fsspec.core.url_to_fs(uri, **(storage.storage_options(cfg) or {}))
    if not fs.exists(path) or not fs.find(path):
        return None
    partitioning = ds.partitioning(pa.schema([("tenant_id", pa.string())]), flavor="hive")
    return ds.dataset(path, filesystem=fs, format="parquet", partitioning=partitioning)


def _migrated(cfg: PlatformConfig, entity: str) -> bool:
    if not catalog.table_exists(cfg, "bronze", entity):
        return False
    return MARKER in catalog.load_catalog(cfg).load_table(f"bronze.{entity}").properties


def migrate_lake(cfg: PlatformConfig) -> dict[str, Any]:
    """Legacy Parquet Bronze -> Iceberg Bronze. Returns ``{entity: rows | reason}``."""
    from idp.pipeline import BRONZE_PARTITIONS, ENTITIES

    now = catalog.now_utc()
    out: dict[str, Any] = {}
    for entity in ENTITIES:
        uri = legacy_bronze_uri(cfg, entity)
        if _migrated(cfg, entity):
            out[entity] = "already migrated"
            continue
        dataset = _legacy_dataset(cfg, uri)
        if dataset is None:
            out[entity] = "no legacy data"
            continue
        # every file's footer, so a column added in a later batch is not lost
        schema = pa.unify_schemas([f.physical_schema for f in dataset.get_fragments()]
                                  + [dataset.partitioning.schema], promote_options="permissive")
        landing = [pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
                   pa.field("ingest_date", pa.date32())]
        schema = pa.schema([f for f in schema if f.name not in ("_ingested_at", "ingest_date")] + landing)
        tenants = sorted(set(dataset.to_table(columns=["tenant_id"])["tenant_id"].to_pylist()))

        def chunks(dataset=dataset, tenants=tenants):
            for tenant in tenants:                        # bounded memory: one tenant at a time
                tbl = dataset.to_table(filter=ds.field("tenant_id") == tenant)
                yield (tbl.append_column("_ingested_at", pa.array([now] * tbl.num_rows, landing[0].type))
                          .append_column("ingest_date", pa.array([now.date()] * tbl.num_rows, pa.date32())))

        out[entity] = catalog.append_chunks(
            cfg, "bronze", entity, schema, chunks(), partition_by=BRONZE_PARTITIONS,
            properties={MARKER: uri, "idp.migrated-at": now.isoformat()})
    return out
