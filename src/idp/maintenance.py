"""Lake maintenance (REQ-ORC-06): keep the Iceberg tables efficient and bounded.

For every table in the ``bronze``/``silver``/``gold`` namespaces:

* **compaction** — ``rewrite_data_files`` (binpack) folds the many small files
  that frequent Bronze appends and Silver merges produce;
* **snapshot expiry** — ``expire_snapshots`` older than
  ``lake.snapshot_retention_days`` (always keeping the current snapshot). For a
  Bronze table the cutoff is capped at the oldest snapshot any Silver streaming
  checkpoint still references, so the next incremental run can always resume;
* **orphan cleanup** — ``remove_orphan_files`` older than the retention, and never
  younger than 24 hours (Iceberg's guard: a concurrent writer's not-yet-committed
  files must not be mistaken for orphans).

None of these changes a table's current contents; Bronze records are never
deleted (compaction rewrites them into bigger files). Bronze *archiving* (moving
old objects to a colder storage class) is object-store policy, not code:
``archive_policy`` renders the rule and ``make lake-archive-policy`` applies it.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from idp.common import catalog
from idp.common.config import PlatformConfig
from idp.transforms import incremental, lake


def archive_policy(cfg: PlatformConfig, *, storage_class: str = "GLACIER_IR",
                   fmt: str = "s3") -> dict[str, Any]:
    """The Bronze archive rule: objects under ``tables/`` older than
    ``lake.bronze_archive_days`` transition to ``storage_class``.

    ``fmt="s3"`` is an S3 lifecycle configuration
    (``aws s3api put-bucket-lifecycle-configuration``); ``fmt="minio"`` is the
    ``mc ilm import`` form, where ``storage_class`` names a configured remote tier.
    The target class must keep objects readable (S3 Glacier Instant Retrieval, a
    MinIO warm tier) so a full refresh can still replay all of Bronze.
    """
    days = int(cfg.lake["bronze_archive_days"])
    rule: dict[str, Any] = {"ID": "idp-bronze-archive", "Status": "Enabled",
                            "Filter": {"Prefix": "tables/"}}
    if fmt == "s3":
        rule["Transitions"] = [{"Days": days, "StorageClass": storage_class}]
    elif fmt == "minio":
        rule["Transition"] = {"Days": days, "StorageClass": storage_class}
    else:
        raise ValueError(f"unknown archive policy format {fmt!r}; expected 's3' or 'minio'")
    return {"Rules": [rule]}


def _ts(dt: datetime) -> str:
    """A UTC timestamp literal. The explicit offset matters: an unqualified
    ``TIMESTAMP '...'`` is read in the Spark session time zone."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+00:00"


def _hadoop_fs(spark: Any, path: str):
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    return hpath, hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())


def checkpoint_snapshot_ids(spark: Any, cfg: PlatformConfig) -> dict[str, set[int]]:
    """Bronze snapshot ids referenced by the latest offset of each Silver checkpoint."""
    refs: dict[str, set[int]] = {}
    for fact, (sources, _) in incremental.FACTS.items():
        for source in sources:
            offsets = f"{incremental.checkpoint_dir(cfg, fact, source)}/offsets"
            hpath, fs = _hadoop_fs(spark, offsets)
            if not fs.exists(hpath):
                continue
            batches = [s.getPath().getName() for s in fs.listStatus(hpath)]
            batches = sorted(int(b) for b in batches if b.isdigit())
            if not batches:
                continue
            latest, _ = _hadoop_fs(spark, f"{offsets}/{batches[-1]}")
            stream = fs.open(latest)
            try:
                text = spark.sparkContext._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
            finally:
                stream.close()
            for line in text.splitlines():
                if line.startswith("{") and "snapshot_id" in line:
                    refs.setdefault(source, set()).add(int(json.loads(line)["snapshot_id"]))
    return refs


def _expiry_cutoff(cfg: PlatformConfig, layer: str, table: str, default: datetime,
                   refs: dict[str, set[int]]) -> datetime:
    """Never expire a Bronze snapshot a checkpoint still needs (or anything newer)."""
    if layer != "bronze" or table not in refs:
        return default
    t = catalog.load_catalog(cfg).load_table(f"bronze.{table}")
    kept = [s.timestamp_ms for s in t.snapshots() if s.snapshot_id in refs[table]]
    if not kept:
        return default
    oldest_needed = datetime.fromtimestamp(min(kept) / 1000, tz=timezone.utc)
    return min(default, oldest_needed)


def maintain(cfg: PlatformConfig) -> dict[str, dict[str, int]]:
    """Compact, expire and clean every lake table. Returns per-table statistics."""
    from idp.transforms.session import get_spark

    spark = get_spark(cfg, app_name="idp-maintain")
    lake.attach(spark, cfg)
    cat_name = catalog.catalog_name(cfg)
    retention = timedelta(days=float(cfg.lake["snapshot_retention_days"]))
    orphan_age = max(retention, timedelta(days=1))
    target_bytes = int(cfg.lake["target_file_size_mb"]) * 1024 * 1024
    now = datetime.now(timezone.utc)
    refs = checkpoint_snapshot_ids(spark, cfg)
    stats: dict[str, dict[str, int]] = {}
    pycat = catalog.load_catalog(cfg)
    for layer in catalog.LAYERS:
        for _, table in pycat.list_tables(layer):
            ident = f"{layer}.{table}"
            files_before = len(list(pycat.load_table(ident).scan().plan_files()))
            spark.sql(
                f"CALL {cat_name}.system.rewrite_data_files(table => '{ident}', "
                f"options => map('target-file-size-bytes', '{target_bytes}', 'min-input-files', '2'))"
            ).collect()
            cutoff = _expiry_cutoff(cfg, layer, table, now - retention, refs)
            expired = spark.sql(
                f"CALL {cat_name}.system.expire_snapshots(table => '{ident}', "
                f"older_than => TIMESTAMP '{_ts(cutoff)}', retain_last => 1)"
            ).collect()
            spark.sql(
                f"CALL {cat_name}.system.remove_orphan_files(table => '{ident}', "
                f"older_than => TIMESTAMP '{_ts(now - orphan_age)}')"
            ).collect()
            stats[ident] = {
                "files_before": files_before,
                "files_after": len(list(pycat.load_table(ident).scan().plan_files())),
                "deleted_data_files": int(expired[0]["deleted_data_files_count"]) if expired else 0,
                "snapshots": len(pycat.load_table(ident).snapshots()),
            }
    return stats
