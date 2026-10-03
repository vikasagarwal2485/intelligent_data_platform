"""Kafka consumer: TMF create events -> raw Bronze (REQ-STR-04).

Batch-drains every ``t.<tenant>.<resource>`` topic, customers included, with the
consumer group (stops after ``idle_timeout`` seconds with no new messages), and
lands each message as one raw event row in the Bronze table of its topic
(``bronze.tmf_<resource>``, ``ingestion.raw``): the value exactly as received, plus
its envelope and Kafka metadata. Nothing is validated, resolved or adapted here;
Silver's conform step does that, and quarantines what fails (REQ-SLV-09).

Only a message that can't be stored as an event (not a UTF-8 JSON object with
``eventType``, ``eventId`` and one resource in ``event``) goes to ``t.<tenant>.dlq``
with the reason. Offsets are committed only after Bronze and the DLQ are written.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, NamedTuple

import pandas as pd

from idp.ingestion.raw import NotAnEvent, bronze_tables, event_row, rows_frame


class DeadLetter(NamedTuple):
    """A message that wasn't landed, and why."""
    tenant: str
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    reason: str


class ConsumeResult(NamedTuple):
    frames: dict[str, pd.DataFrame]          # raw Bronze table -> event rows landed
    dead_letters: list[DeadLetter]           # sent to t.<tenant>.dlq


def _split(topic: str) -> tuple[str, str]:
    """``<prefix>.<tenant>.<suffix>`` -> (tenant, suffix)."""
    _, tenant, suffix = topic.split(".", 2)
    return tenant, suffix


def _write_dead_letters(bootstrap: str, dead: list[DeadLetter], topic_prefix: str) -> None:
    from kafka import KafkaProducer
    from idp.streaming import DLQ_SUFFIX

    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
    try:
        for d in dead:
            headers = [("idp.reason", d.reason.encode("utf-8")),
                       ("idp.source.topic", d.topic.encode("utf-8")),
                       ("idp.source.partition", str(d.partition).encode("utf-8")),
                       ("idp.source.offset", str(d.offset).encode("utf-8"))]
            producer.send(f"{topic_prefix}.{d.tenant}.{DLQ_SUFFIX}", key=d.key, value=d.value,
                          headers=headers)
        producer.flush()
    finally:
        producer.close()


def consume_to_bronze(
    bootstrap: str,
    tenants: list[str],
    *,
    topic_prefix: str = "t",
    idle_timeout: float = 15.0,
    group_id: str = "idp-bronze-loader",
    land: Callable[[dict[str, pd.DataFrame]], Any] | None = None,
) -> ConsumeResult:
    """Consume the TMF event topics and return the raw rows per Bronze table and the
    dead letters.

    With ``land`` (e.g. ``lambda frames: to_bronze(cfg, frames)``), the drained
    batch's rows are handed to it, the dead letters are written to the DLQ topics,
    and the Kafka offsets are committed ONLY after both succeed: if either fails,
    nothing is committed and the next consume re-reads the batch (Bronze duplicates
    are harmless; Silver keeps one version per event). Without ``land`` nothing is
    written or committed.
    """
    from kafka import KafkaConsumer

    tables = bronze_tables()
    topics = [f"{topic_prefix}.{t}.{suffix}" for t in tenants for suffix in tables]
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,           # commit only after Bronze and the DLQ are written
        consumer_timeout_ms=int(idle_timeout * 1000),
    )
    rows: dict[str, list[dict]] = defaultdict(list)
    dead: list[DeadLetter] = []
    try:
        for msg in consumer:  # ends after idle_timeout with no messages
            tenant, suffix = _split(msg.topic)
            try:
                rows[tables[suffix]].append(
                    event_row(tenant, msg.topic, msg.value, msg.key, msg.partition, msg.offset))
            except NotAnEvent as e:
                dead.append(DeadLetter(tenant, msg.topic, msg.partition, msg.offset,
                                       msg.key, msg.value, str(e)))
        frames = {table: rows_frame(rs) for table, rs in rows.items() if rs}
        if land is not None and (frames or dead):
            if frames:
                land(frames)                # raises -> offsets stay uncommitted
            if dead:
                _write_dead_letters(bootstrap, dead, topic_prefix)
            consumer.commit()
    finally:
        consumer.close()
    return ConsumeResult(frames, dead)
