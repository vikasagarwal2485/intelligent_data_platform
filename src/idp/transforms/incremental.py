"""Incremental Silver: Bronze -> Silver with only the new Bronze (REQ-SLV-07/08).

* ``dim_subscriber`` (one row per subscriber) is rebuilt in full from the Bronze
  reference tables each run — its cost is bounded by the subscriber count.
* The event facts (``fact_usage`` / ``fact_recharge`` / ``fact_fee_charge``)
  process only the Bronze snapshots appended since the previous successful run:
  a Structured Streaming query per Bronze source, ``Trigger.Once``, a
  durable checkpoint, and a ``foreachBatch`` that applies the same pure transform
  and merges on the natural key ``(tenant_id, id)`` (insert-if-absent). The
  checkpoint advances only after the merge commits, and the merge never inserts a
  key twice, so a failed run is resumed without loss or duplication.
* A full refresh drops the checkpoints and the facts and replays all of Bronze
  (the initial offset scans every live file, so expired snapshots lose nothing).

``Trigger.Once`` rather than ``AvailableNow``: Iceberg's AvailableNow re-validates
the stream's *first* Bronze snapshot on every restart, so snapshot expiry would
break it (see the iceberg-incremental-lakehouse design, decision 4).
"""
from __future__ import annotations

from typing import Any

from idp.common import catalog
from idp.common.config import PlatformConfig
from idp.transforms import lake, silver

# Bronze landing metadata (REQ-BRZ-01) is not part of the Silver schema.
LANDING_COLUMNS = ("_ingested_at", "ingest_date")
FACT_KEYS = ("tenant_id", "id")

# fact -> (ordered Bronze sources, event-time column). Order matters for the fact's
# column order: voice carries every usage column, so it creates fact_usage.
FACTS: dict[str, tuple[tuple[str, ...], str]] = {
    "fact_usage": (("voice_events", "data_events"), "started_at"),
    "fact_recharge": (("recharge_events",), "created_at"),
    "fact_fee_charge": (("fee_charges",), "charged_at"),
}
REFERENCE = ("parties", "accounts", "agreements", "products")


def _strip_landing(df: Any) -> Any:
    return df.drop(*[c for c in LANDING_COLUMNS if c in df.columns])


def checkpoint_dir(cfg: PlatformConfig, fact: str, source: str) -> str:
    return f"{lake.layer_uri(cfg, 'silver')}/_checkpoints/{fact}/{source}"


def _delete_path(spark: Any, path: str) -> None:
    """Recursively delete ``path`` via Hadoop FS (works for local and s3a)."""
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if fs.exists(hpath):
        fs.delete(hpath, True)


def _fact_transform(fact: str, salts: dict[str, str], products: Any | None):
    if fact == "fact_usage":
        return lambda batch: silver.build_fact_usage(batch, None, salts)
    if fact == "fact_recharge":
        return lambda batch: silver.build_fact_recharge(batch, salts)
    return lambda batch: silver.build_fact_fee_charge(batch, products)


def build_dim(spark: Any, cfg: PlatformConfig, salts: dict[str, str], *, full_refresh: bool) -> None:
    """Rebuild ``dim_subscriber`` from the Bronze reference tables."""
    ref = {}
    for entity in REFERENCE:
        df = lake.read_table(spark, cfg, "bronze", entity)
        ref[entity] = None if df is None or df.isEmpty() else _strip_landing(df)
    missing = [e for e in silver.REQUIRED_ENTITIES if ref.get(e) is None]
    if missing:
        raise RuntimeError(
            f"Bronze is missing entity tables {missing}. Populate Bronze first "
            "('idp produce' then 'idp consume')."
        )
    dim = silver.build_dim_subscriber(ref["parties"], ref["accounts"], ref["agreements"],
                                      ref["products"], salts)
    write = lake.replace_all if full_refresh else lake.overwrite_tenants
    write(dim, cfg, "silver", "dim_subscriber")


def process_facts(spark: Any, cfg: PlatformConfig, salts: dict[str, str]) -> dict[str, int]:
    """Run each fact's streams to the end of the available Bronze. Returns the
    number of Bronze rows each fact processed in this run."""
    from pyspark.sql import functions as F

    products = lake.read_table(spark, cfg, "bronze", "products")
    products = None if products is None else _strip_landing(products)
    processed: dict[str, int] = {}
    for fact, (sources, event_ts) in FACTS.items():
        transform = _fact_transform(fact, salts, products)
        processed[fact] = 0
        for source in sources:
            if not catalog.table_exists(cfg, "bronze", source):
                continue
            seen: list[int] = []

            def merge_batch(batch: Any, _batch_id: int, fact=fact, transform=transform,
                            event_ts=event_ts, seen=seen) -> None:
                n = batch.count()
                if n == 0:
                    return
                seen.append(n)
                out = transform(_strip_landing(batch))
                lake.merge_insert_new(out, cfg, "silver", fact, keys=FACT_KEYS,
                                      partition_by=("tenant_id", F.days(event_ts)))

            query = (spark.readStream.format("iceberg")
                     .option("streaming-skip-overwrite-snapshots", "true")
                     .option("streaming-skip-delete-snapshots", "true")
                     .load(catalog.spark_table(cfg, "bronze", source))
                     .writeStream.foreachBatch(merge_batch)
                     .option("checkpointLocation", checkpoint_dir(cfg, fact, source))
                     .trigger(once=True)
                     .start())
            try:
                query.awaitTermination()
            except Exception as e:                     # noqa: BLE001 - StreamingQueryException
                # the checkpoint was not advanced; the next run re-processes this Bronze
                raise RuntimeError(f"Silver {fact} from bronze.{source} failed: {e}") from e
            processed[fact] += sum(seen)
    return processed


def reset_facts(spark: Any, cfg: PlatformConfig) -> None:
    """Full refresh: drop the fact checkpoints and empty the facts (one commit each)."""
    for fact, (sources, _) in FACTS.items():
        for source in sources:
            _delete_path(spark, checkpoint_dir(cfg, fact, source))
        if catalog.table_exists(cfg, "silver", fact):
            spark.sql(f"DELETE FROM {catalog.spark_table(cfg, 'silver', fact)}")
