"""Kafka producer: publish synthetic data to per-tenant topics (plan 3.1/3.2).

Publishes each entity/event record as a JSON message to
``t.<tenant>.<entity>`` (e.g. ``t.tenant_a.usage.voice``), keyed by the
record's natural key so partitioning is stable per subscriber.
"""
from __future__ import annotations

import json
from typing import Iterable

import pandas as pd

from idp.synthetic.generator import SyntheticDataset


def _topic(prefix: str, tenant_id: str, suffix: str) -> str:
    return f"{prefix}.{tenant_id}.{suffix}"


def _key_for(table: str, row: dict) -> bytes:
    # subscriber-centric key where available, else the record id
    key = row.get("service_id") or row.get("msisdn") or row.get("id") or ""
    return str(key).encode("utf-8")


def publish_records(
    items: Iterable[tuple[str, Iterable[dict]]],
    bootstrap: str,
    *,
    topic_prefix: str = "t",
    acks: str | int = "all",
) -> dict[str, int]:
    """Publish ``(table, records)`` pairs to Kafka. Returns per-topic message counts.

    The single publishing path: each record goes to ``t.<tenant>.<suffix>`` keyed by
    its natural key, serialized as JSON (``default=str``). In-memory production and
    publishing from the landing zone both come through here, so the same records
    produce the same messages (REQ-STR-01).
    """
    from kafka import KafkaProducer  # imported lazily so non-streaming use needs no dep
    from idp.streaming import TABLE_TO_SUFFIX

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks=acks,
        linger_ms=50,
        retries=3,
    )
    counts: dict[str, int] = {}
    try:
        for table, records in items:
            suffix = TABLE_TO_SUFFIX[table]
            for rec in records:
                topic = _topic(topic_prefix, rec["tenant_id"], suffix)
                producer.send(topic, key=_key_for(table, rec), value=rec)
                counts[topic] = counts.get(topic, 0) + 1
        producer.flush()
    finally:
        producer.close()
    return counts


def produce_to_kafka(
    dataset: SyntheticDataset,
    bootstrap: str,
    *,
    topic_prefix: str = "t",
    acks: str | int = "all",
) -> dict[str, int]:
    """Publish all entity/event tables to Kafka. Returns per-topic message counts."""
    from idp.streaming import TABLE_TO_SUFFIX

    def items():
        for table in TABLE_TO_SUFFIX:
            df: pd.DataFrame = getattr(dataset, table)
            if df is None or df.empty:
                continue
            yield table, df.to_dict("records")  # faster than iterrows for big frames

    return publish_records(items(), bootstrap, topic_prefix=topic_prefix, acks=acks)
