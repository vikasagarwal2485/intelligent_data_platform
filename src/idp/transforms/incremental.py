"""Incremental Silver: raw Bronze -> conformed -> Silver (REQ-SLV-07/08/09/10).

* The **dimensions** (``dim_subscriber``, ``dim_customer``, ``dim_billing_account``,
  ``dim_product``) are rebuilt in full each run from the latest version of each
  reference resource in Bronze, conformed on the driver
  (``conform.conform_reference``); their cost is bounded by the subscriber count.
* The **event facts** (``fact_usage`` / ``fact_recharge`` / ``fact_fee_charge``)
  process only the raw Bronze events appended since the previous successful run: a
  Structured Streaming query per raw table, ``Trigger.Once``, a durable checkpoint,
  and a ``foreachBatch`` that conforms the batch on the executors
  (``conform.conform_facts``), applies the same pure transform and merges on the
  natural key ``(tenant_id, id)`` (insert-if-absent). The checkpoint advances only
  after the merge commits, and the merge never inserts a key twice, so a failed run
  is resumed without loss or duplication.
* Events that fail conforming go to ``silver.quarantine`` (merged on
  ``(tenant_id, source_table, event_id)``), never to another Silver table.
* A full refresh drops the checkpoints, the facts and the quarantine, and replays
  all of Bronze (the initial offset scans every live file, so expired snapshots lose
  nothing).

``Trigger.Once`` rather than ``AvailableNow``: Iceberg's AvailableNow re-validates
the stream's *first* Bronze snapshot on every restart, so snapshot expiry would
break it (see the iceberg-incremental-lakehouse design, decision 4).
"""
from __future__ import annotations

from typing import Any

from idp.common import catalog
from idp.common.config import PlatformConfig
from idp.transforms import conform, lake, silver

# Bronze landing metadata (REQ-BRZ-01) is not part of the Silver schema.
LANDING_COLUMNS = ("_ingested_at", "ingest_date")
FACT_KEYS = ("tenant_id", "id")

# fact -> (raw Bronze source table, event-time column)
FACTS: dict[str, tuple[str, str]] = {
    "fact_usage": ("tmf_product_usage", "started_at"),
    "fact_recharge": ("tmf_topup_balance", "created_at"),
    "fact_fee_charge": ("tmf_applied_customer_billing_rate", "charged_at"),
}
DIMENSIONS = ("dim_subscriber", "dim_customer", "dim_billing_account", "dim_product")
QUARANTINE = "quarantine"


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


def write_quarantine(df: Any, cfg: PlatformConfig) -> int:
    """Merge quarantine rows into ``silver.quarantine``; returns how many were given."""
    n = df.count()
    if n:
        lake.merge_insert_new(conform.quarantine_frame(df.sparkSession, df), cfg, "silver", QUARANTINE,
                              keys=conform.QUARANTINE_KEYS)
    return n


def build_dims(spark: Any, cfg: PlatformConfig, salts: dict[str, str], *, full_refresh: bool,
               profile: str | None = None) -> tuple[Any, dict[str, int]]:
    """Conform the reference entities and rebuild the dimensions. Returns the
    conformed products (for fee enrichment) and the quarantine count per raw table."""
    from collections import Counter

    entities, quarantine = conform.conform_reference(spark, cfg, profile)
    dims = {
        "dim_subscriber": silver.build_dim_subscriber(entities["parties"], entities["customers"],
                                                      entities["billing_accounts"], entities["agreements"],
                                                      entities.get("products"), salts),
        "dim_customer": silver.build_dim_customer(entities["customers"], entities["parties"]),
        "dim_billing_account": silver.build_dim_billing_account(entities["billing_accounts"]),
        "dim_product": (None if entities.get("products") is None
                        else silver.build_dim_product(entities["products"])),
    }
    write = lake.replace_all if full_refresh else lake.overwrite_tenants
    for name, dim in dims.items():
        if dim is not None:
            write(dim, cfg, "silver", name)
    if len(quarantine):
        write_quarantine(conform.quarantine_frame(spark, quarantine).drop("quarantined_at"), cfg)
    return entities.get("products"), dict(Counter(quarantine["source_table"]))


def process_facts(spark: Any, cfg: PlatformConfig, salts: dict[str, str], products: Any | None = None,
                  profile: str | None = None) -> tuple[dict[str, int], dict[str, int]]:
    """Run each fact's stream to the end of the available raw Bronze. Returns the
    number of raw events each fact processed in this run, and the number quarantined
    per raw table."""
    from pyspark.sql import functions as F

    processed: dict[str, int] = {}
    quarantined: dict[str, int] = {}
    for fact, (source, event_ts) in FACTS.items():
        transform = _fact_transform(fact, salts, products)
        processed[fact] = 0
        if not catalog.table_exists(cfg, "bronze", source):
            continue
        seen: list[int] = []
        bad: list[int] = []

        def merge_batch(batch: Any, _batch_id: int, fact=fact, source=source, transform=transform,
                        event_ts=event_ts, seen=seen, bad=bad) -> None:
            n = batch.count()
            if n == 0:
                return
            seen.append(n)
            ok, rejected, cached = conform.conform_facts(_strip_landing(batch), source, profile)
            try:
                ok = ok.drop("_entity")
                if not ok.isEmpty():
                    lake.merge_insert_new(transform(ok), cfg, "silver", fact, keys=FACT_KEYS,
                                          partition_by=("tenant_id", F.days(event_ts)))
                bad.append(write_quarantine(rejected, cfg))
            finally:
                cached.unpersist()

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
        processed[fact] = sum(seen)
        if sum(bad):
            quarantined[source] = sum(bad)
    return processed, quarantined


def reset_facts(spark: Any, cfg: PlatformConfig) -> None:
    """Full refresh: drop the fact checkpoints, and empty the facts and the quarantine
    (one commit each), so everything is conformed again from all of Bronze."""
    for fact, (source, _) in FACTS.items():
        _delete_path(spark, checkpoint_dir(cfg, fact, source))
        if catalog.table_exists(cfg, "silver", fact):
            spark.sql(f"DELETE FROM {catalog.spark_table(cfg, 'silver', fact)}")
    if catalog.table_exists(cfg, "silver", QUARANTINE):
        spark.sql(f"DELETE FROM {catalog.spark_table(cfg, 'silver', QUARANTINE)}")
