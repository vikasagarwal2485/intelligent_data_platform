"""Raw Bronze rows: one TM Forum event as received (REQ-BRZ-01, REQ-STR-04).

Bronze keeps every event exactly as it arrived, one Iceberg table per Kafka topic
(``bronze.tmf_<resource>``). A row holds the message value verbatim (``payload``)
plus, as columns for pruning and lineage, its tenant (from the topic), the envelope's
``eventId``/``eventType``/``eventTime``, the resource's ``@type`` and id, and the
Kafka position. Nothing is validated, adapted or de-duplicated here: Silver's
conform step does that (REQ-SLV-09), and quarantines what fails.

The consumer builds a row per message with :func:`event_row`. The local paths
(``idp run``, tests, goldens) build the same rows from a generated dataset with
:func:`raw_events`, so every path lands identical Bronze.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any, Iterator

import pandas as pd

from idp.ingestion.tmf.profile import Profile, load_profile

# the columns of every raw Bronze table, in order (Bronze adds _ingested_at, ingest_date)
RAW_COLUMNS = ("tenant_id", "event_id", "event_type", "event_time", "resource_type", "resource_id",
               "kafka_topic", "kafka_partition", "kafka_offset", "kafka_key", "payload")


class NotAnEvent(ValueError):
    """A message that can't be stored as an event (it is dead-lettered instead)."""


def bronze_table(suffix: str) -> str:
    """The raw Bronze table of a topic suffix: ``productUsage`` -> ``tmf_product_usage``."""
    return "tmf_" + re.sub(r"(?<!^)(?=[A-Z])", "_", suffix).lower()


def bronze_tables() -> dict[str, str]:
    """Topic suffix -> raw Bronze table, for every TMF topic."""
    from idp.streaming import TOPIC_RESOURCES
    return {suffix: bronze_table(suffix) for suffix in TOPIC_RESOURCES}


def event_row(tenant: str, topic: str, value: bytes, key: bytes | None,
              partition: int, offset: int) -> dict[str, Any]:
    """The raw Bronze row of one message. Raises :class:`NotAnEvent` when the value
    isn't a UTF-8 JSON object with ``eventType``, ``eventId`` and an ``event`` holding
    exactly one resource object; anything else is stored as received."""
    try:
        text = value.decode("utf-8")
        event = json.loads(text)
    except (UnicodeDecodeError, ValueError) as e:
        raise NotAnEvent(f"invalid json: {e}") from e
    if not isinstance(event, dict):
        raise NotAnEvent("not a JSON object")
    for name in ("eventType", "eventId"):
        if not isinstance(event.get(name), str) or not event[name]:
            raise NotAnEvent(f"missing {name}")
    payload = event.get("event")
    if not isinstance(payload, dict) or len(payload) != 1 or not isinstance(next(iter(payload.values())), dict):
        raise NotAnEvent("event must hold exactly one resource object")
    resource = next(iter(payload.values()))
    rid, event_time = resource.get("id"), event.get("eventTime")
    return {
        "tenant_id": tenant, "event_id": event["eventId"], "event_type": event["eventType"],
        "event_time": event_time if isinstance(event_time, str) else None,
        "resource_type": resource.get("@type") if isinstance(resource.get("@type"), str) else None,
        "resource_id": None if rid is None else str(rid),
        "kafka_topic": topic, "kafka_partition": int(partition), "kafka_offset": int(offset),
        "kafka_key": None if key is None else key.decode("utf-8", errors="replace"),
        "payload": text,
    }


def rows_frame(rows: list[dict]) -> pd.DataFrame:
    """Raw rows as a frame with the raw columns, in order and with stable types."""
    df = pd.DataFrame(rows, columns=list(RAW_COLUMNS))
    return df.astype({"kafka_partition": "int32", "kafka_offset": "int64"})


def raw_events(dataset: Any, profile: Profile | None = None, *, topic_prefix: str = "t",
               event_time: str | None = None) -> dict[str, pd.DataFrame]:
    """A generated dataset as raw Bronze rows per table: exactly the events the
    producer would publish (``tenant_events``), at offsets 0.. per topic, partition 0.
    The local stand-in for produce + consume."""
    from idp.streaming.producer import dataset_files, tenant_events

    profile = profile or load_profile()
    tables = bronze_tables()
    rows: dict[str, list[dict]] = defaultdict(list)
    offsets: dict[str, int] = defaultdict(int)
    for tenant, files in dataset_files(dataset, profile):
        for suffix, key, event in tenant_events(profile, tenant, files, event_time):
            topic = f"{topic_prefix}.{tenant}.{suffix}"
            value = json.dumps(event).encode("utf-8")
            rows[tables[suffix]].append(event_row(tenant, topic, value, key, 0, offsets[topic]))
            offsets[topic] += 1
    return {table: rows_frame(rs) for table, rs in rows.items()}


def payloads(df: pd.DataFrame) -> Iterator[dict]:
    """The events of raw rows, parsed."""
    for text in df["payload"]:
        yield json.loads(text)
