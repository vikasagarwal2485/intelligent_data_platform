"""Silver's conform step: raw TMF events in Bronze -> canonical records (REQ-SLV-09/10).

Bronze holds every event as received (``bronze.tmf_<resource>``, REQ-BRZ-01). Before
Silver curates anything, each event is:

1. checked: a create event of its table's resource type, its resource valid against
   the TMF and extension schemas, and its ``tenantId`` equal to the event's tenant
   (``ingestion.tmf.events.from_event``);
2. adapted with the mapping profile (``adapt_resource``, REQ-BRZ-05), references
   resolved against the tenant's latest customers;
3. checked against its entity's canonical Avro contract (``common.contracts``).

An event that fails goes to ``silver.quarantine`` with the reason, never to another
Silver table, and doesn't fail the run. The reasons name fields and rules, not the
offending values, so the quarantine holds no PII (REQ-SLV-03); the event itself is
in Bronze, found by ``event_id``.

**Reference entities** (parties, customers, billing accounts, agreements, products)
are rebuilt every run, like the dimensions: the latest event per
``(tenant_id, resource_id)`` by ``event_time``, then ``_ingested_at``, then the Kafka
offset (REQ-SLV-10), conformed on the driver (bounded by the subscriber count).
**Fact events** (usage, recharges, fee charges) are conformed per micro-batch on the
executors (``mapInPandas``); none of them reads a customer.
"""
from __future__ import annotations

import functools
import json
from collections import defaultdict
from typing import Any, Iterable, Iterator

import pandas as pd

from idp.common import contracts
from idp.ingestion.raw import bronze_tables
from idp.ingestion.tmf.events import from_event
from idp.ingestion.tmf.profile import Profile, _Customers, adapt_resource, customer_index, load_profile

# canonical reference entity -> its raw Bronze table, in conform order (customers first:
# the others resolve their references through them)
REFERENCE_TABLES = {
    "customers": "tmf_customer",
    "parties": "tmf_party",
    "billing_accounts": "tmf_billing_account",
    "agreements": "tmf_agreement",
    "products": "tmf_product",
}
# raw Bronze table of each fact source -> the canonical entities it yields
FACT_TABLES = {
    "tmf_product_usage": ("voice_events", "data_events"),
    "tmf_topup_balance": ("recharge_events",),
    "tmf_applied_customer_billing_rate": ("fee_charges",),
}
REQUIRED_TABLES = ("tmf_party", "tmf_customer", "tmf_billing_account", "tmf_agreement")
QUARANTINE_COLUMNS = ("tenant_id", "source_table", "event_id", "resource_type", "resource_id",
                      "reason", "event_time")
QUARANTINE_KEYS = ("tenant_id", "source_table", "event_id")


def _table_suffix() -> dict[str, str]:
    return {table: suffix for suffix, table in bronze_tables().items()}


@functools.lru_cache(maxsize=None)
def _profile(name: str | None) -> Profile:
    """The mapping profile, loaded once per process (driver and executors)."""
    return load_profile(name)


# --------------------------------------------------------------------------- #
# per event (pure Python: the same code on the driver and the executors)       #
# --------------------------------------------------------------------------- #
def _quarantine(row: dict, table: str, reason: str) -> dict:
    return {"tenant_id": row["tenant_id"], "source_table": table, "event_id": row.get("event_id"),
            "resource_type": row.get("resource_type"), "resource_id": row.get("resource_id"),
            "reason": reason, "event_time": row.get("event_time")}


def resource_of(row: dict, table: str) -> dict:
    """The validated resource of a raw row (raises ValueError with the reason)."""
    from idp.streaming import TOPIC_RESOURCES

    try:
        event = json.loads(row["payload"])
    except ValueError as e:
        raise ValueError(f"invalid json: {e}") from e
    return from_event(event, TOPIC_RESOURCES[_table_suffix()[table]], row["tenant_id"])


def conform_rows(rows: Iterable[dict], table: str, profile: Profile,
                 customers: dict[str, _Customers] | None = None) -> tuple[dict[str, list[dict]], list[dict]]:
    """Raw rows of one Bronze table -> (canonical records per entity, quarantine rows)."""
    customers = customers or {}
    out: dict[str, list[dict]] = defaultdict(list)
    bad: list[dict] = []
    empty = customer_index(profile, []) if any(f.via for r in profile.resources for f in r.fields) else None
    for row in rows:
        try:
            resource = resource_of(row, table)
            entity, record = adapt_resource(profile, resource, customers.get(row["tenant_id"], empty))
            err = contracts.error(entity, record)
            if err:
                raise ValueError(f"breaks the canonical contract: {err}")
        except ValueError as e:                     # EventError, ReferenceError_, contract
            bad.append(_quarantine(row, table, str(e)))
            continue
        out[entity].append(record)
    return dict(out), bad


# --------------------------------------------------------------------------- #
# reference entities: latest version, conformed on the driver                  #
# --------------------------------------------------------------------------- #
def latest(df: Any) -> Any:
    """The latest event per ``(tenant_id, resource_id)`` (REQ-SLV-10): newest
    ``event_time``, then landed last (``_ingested_at``), then the highest offset."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = [F.to_timestamp("event_time").desc_nulls_last(), F.col("_ingested_at").desc_nulls_last(),
             F.col("kafka_offset").desc_nulls_last()]
    w = Window.partitionBy("tenant_id", "resource_id").orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _records_frame(spark: Any, entity: str, records: list[dict]) -> Any:
    name = contracts.ENTITY_SCHEMA[entity]
    pdf = pd.DataFrame(records, columns=contracts.fields(name))
    return spark.createDataFrame(pdf.astype(object).where(pdf.notna(), None),
                                 schema=contracts.spark_schema(name))


def conform_reference(spark: Any, cfg: Any, profile_name: str | None = None) -> tuple[dict[str, Any], pd.DataFrame]:
    """The conformed reference entities as Spark frames (absent tables -> None), and
    their quarantine rows. Raises when a required raw table is missing or empty."""
    from idp.transforms import lake

    profile = _profile(profile_name)
    raw: dict[str, list[dict]] = {}
    for entity, table in REFERENCE_TABLES.items():
        df = lake.read_table(spark, cfg, "bronze", table)
        raw[entity] = [] if df is None else [r.asDict() for r in latest(df).collect()]
    missing = [t for e, t in REFERENCE_TABLES.items() if t in REQUIRED_TABLES and not raw[e]]
    if missing:
        raise RuntimeError(f"Bronze is missing the raw tables {missing}. Populate Bronze first "
                           "('idp produce' then 'idp consume').")
    # customers first: their valid resources are the index the others resolve through
    valid_customers: dict[str, list[dict]] = defaultdict(list)
    for row in raw["customers"]:
        try:
            valid_customers[row["tenant_id"]].append(resource_of(row, "tmf_customer"))
        except ValueError:
            pass                                    # quarantined by conform_rows below
    index = {t: customer_index(profile, rs) for t, rs in valid_customers.items()}
    frames: dict[str, Any] = {}
    quarantine: list[dict] = []
    for entity, table in REFERENCE_TABLES.items():
        records, bad = conform_rows(raw[entity], table, profile, index)
        quarantine += bad
        frames[entity] = _records_frame(spark, entity, records.get(entity, [])) if raw[entity] else None
    return frames, pd.DataFrame(quarantine, columns=list(QUARANTINE_COLUMNS))


# --------------------------------------------------------------------------- #
# fact events: per micro-batch, on the executors                               #
# --------------------------------------------------------------------------- #
def _fact_out_schema(table: str) -> Any:
    from pyspark.sql import types as T

    contract = contracts.ENTITY_SCHEMA[FACT_TABLES[table][0]]
    fields = list(contracts.spark_schema(contract).fields)
    for f in fields:
        f.nullable = True                           # quarantined rows carry no record
    meta = [T.StructField(c, T.StringType(), True) for c in ("_entity", *("_q_" + c for c in QUARANTINE_COLUMNS))]
    return T.StructType(fields + meta)


def _conform_partitions(table: str, profile_name: str | None):
    contract = contracts.ENTITY_SCHEMA[FACT_TABLES[table][0]]
    columns = contracts.fields(contract)
    q_cols = ["_q_" + c for c in QUARANTINE_COLUMNS]

    def run(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        profile = _profile(profile_name)
        for pdf in batches:
            rows = pdf.astype(object).where(pdf.notna(), None).to_dict("records")
            records, bad = conform_rows(rows, table, profile)
            out = [{**r, "_entity": entity, **{c: None for c in q_cols}}
                   for entity, rs in records.items() for r in rs]
            out += [{**{c: None for c in columns}, "_entity": None,
                     **{"_q_" + k: v for k, v in q.items()}} for q in bad]
            yield pd.DataFrame(out, columns=[*columns, "_entity", *q_cols])

    return run


def conform_facts(batch: Any, table: str, profile_name: str | None = None) -> tuple[Any, Any, Any]:
    """A batch of raw fact rows -> (conformed records with ``_entity``, quarantine rows,
    the cached combined frame), as Spark frames, conformed once on the executors.
    The caller unpersists the third when it has written the other two."""
    from pyspark.sql import functions as F

    raw_cols = [c for c in batch.columns if not c.startswith("_") and c != "ingest_date"]
    out = (batch.select(*raw_cols)
                .mapInPandas(_conform_partitions(table, profile_name), schema=_fact_out_schema(table))
                .persist())
    ok = out.filter(F.col("_q_reason").isNull()).drop(*[c for c in out.columns if c.startswith("_q_")])
    bad = (out.filter(F.col("_q_reason").isNotNull())
              .select(*[F.col("_q_" + c).alias(c) for c in QUARANTINE_COLUMNS]))
    return ok, bad, out


def quarantine_frame(spark: Any, rows: pd.DataFrame | Any) -> Any:
    """Quarantine rows (pandas or Spark) as the Spark frame written to
    ``silver.quarantine``, stamped with the time they were quarantined."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    if isinstance(rows, pd.DataFrame):
        schema = T.StructType([T.StructField(c, T.StringType(), c not in QUARANTINE_KEYS)
                               for c in QUARANTINE_COLUMNS])
        rows = spark.createDataFrame(rows.astype(object).where(rows.notna(), None), schema=schema)
    return rows.withColumn("quarantined_at", F.current_timestamp())
