"""Kafka consumer: TMF create events -> canonical Bronze (REQ-STR-02).

1. Reads each tenant's log-compacted ``customer`` topic in full (no consumer group,
   from the beginning to the end offsets at start), keeping each customer's last
   event. So a customer published in an earlier run still resolves.
2. Batch-drains the resource topics with the consumer group (stops after
   ``idle_timeout`` seconds with no new messages). Each message is checked (the
   create event of its topic, the resource's schema, its ``tenantId``), adapted to
   a canonical record by the TMF adapter (REQ-BRZ-05) with the tenant's customers,
   and checked against its entity's canonical contract (``config/avro``).
3. Lands the records (one Bronze append per entity), sends every message that
   failed a check to ``t.<tenant>.dlq`` with the reason, and only then commits
   the offsets.

Only canonical records are kept in memory, never the (larger) TMF payloads.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from typing import Any, Callable, NamedTuple

import pandas as pd

from idp.common import contracts
from idp.ingestion.tmf.events import from_event
from idp.ingestion.tmf.profile import Profile, adapt_resource, customer_index, load_profile


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
    frames: dict[str, pd.DataFrame]          # canonical entity -> records landed
    dead_letters: list[DeadLetter]           # sent to t.<tenant>.dlq
    customer_problems: list[str]             # invalid customer events (left out of the index)


def _split(topic: str) -> tuple[str, str]:
    """``<prefix>.<tenant>.<suffix>`` -> (tenant, suffix)."""
    _, tenant, suffix = topic.split(".", 2)
    return tenant, suffix


def read_customers(bootstrap: str, tenants: list[str], *, topic_prefix: str = "t",
                   timeout: float = 30.0) -> tuple[dict[str, list[dict]], list[str]]:
    """Every tenant's customers from its compacted topic: the last valid event of
    each customer id, read from the beginning to the end offsets taken at start.
    Returns ``({tenant: [Customer resources]}, [problems])``; an invalid event is a
    problem and left out (an earlier valid event of the same customer stays)."""
    from kafka import KafkaConsumer, TopicPartition
    from idp.streaming import CUSTOMER_SUFFIX

    consumer = KafkaConsumer(bootstrap_servers=bootstrap, group_id=None,
                             enable_auto_commit=False, auto_offset_reset="earliest")
    latest: dict[str, dict[str, dict]] = {t: {} for t in tenants}
    problems: list[str] = []
    try:
        tps = [TopicPartition(f"{topic_prefix}.{t}.{CUSTOMER_SUFFIX}", p)
               for t in tenants
               for p in sorted(consumer.partitions_for_topic(f"{topic_prefix}.{t}.{CUSTOMER_SUFFIX}") or ())]
        if not tps:
            return {t: [] for t in tenants}, problems
        consumer.assign(tps)
        consumer.seek_to_beginning(*tps)
        end = consumer.end_offsets(tps)
        pending = {tp for tp in tps if end[tp] > consumer.position(tp)}
        deadline = time.monotonic() + timeout
        while pending:
            if time.monotonic() > deadline:
                raise TimeoutError(f"customer topics not read to their end within {timeout}s: "
                                   f"{sorted(f'{tp.topic}[{tp.partition}]' for tp in pending)}")
            for tp, msgs in consumer.poll(timeout_ms=1000).items():
                tenant, _ = _split(tp.topic)
                for m in msgs:
                    if m.offset >= end[tp]:
                        continue
                    try:
                        res = from_event(json.loads(m.value), ("Customer",), tenant)
                    except ValueError as e:
                        problems.append(f"{tp.topic}[{tp.partition}]@{m.offset}: {e}")
                        continue
                    latest[tenant][res.get("id")] = res
            pending = {tp for tp in pending if consumer.position(tp) < end[tp]}
    finally:
        consumer.close()
    return {t: list(by_id.values()) for t, by_id in latest.items()}, problems


def _adapt(profile: Profile, customers: dict[str, Any], tenant: str, suffix: str,
           raw: bytes) -> tuple[str, dict]:
    """One message -> (entity, canonical record); raises ValueError with the reason."""
    from idp.streaming import TOPIC_RESOURCES

    try:
        value = json.loads(raw)
    except ValueError as e:
        raise ValueError(f"invalid json: {e}") from e
    resource = from_event(value, TOPIC_RESOURCES[suffix], tenant)
    entity, record = adapt_resource(profile, resource, customers[tenant])
    err = contracts.error(entity, record)
    if err:
        raise ValueError(f"breaks the canonical contract: {err}")
    return entity, record


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
    profile: Profile | None = None,
    land: Callable[[dict[str, pd.DataFrame]], Any] | None = None,
) -> ConsumeResult:
    """Consume the TMF resource topics and return the canonical frames, the dead
    letters and the customer problems.

    With ``land`` (e.g. ``lambda frames: to_bronze(cfg, frames)``), the drained
    batch's records are handed to it, the dead letters are written to the DLQ
    topics, and the Kafka offsets are committed ONLY after both succeed: if either
    fails, nothing is committed and the next consume re-reads the batch (Bronze
    duplicates are harmless; Silver merges on natural keys). Without ``land``
    nothing is written or committed.
    """
    from kafka import KafkaConsumer
    from idp.streaming import CUSTOMER_SUFFIX, TOPIC_RESOURCES

    profile = profile or load_profile()
    by_tenant, problems = read_customers(bootstrap, tenants, topic_prefix=topic_prefix)
    customers = {t: customer_index(profile, by_tenant.get(t, [])) for t in tenants}
    topics = [f"{topic_prefix}.{t}.{suffix}" for t in tenants for suffix in TOPIC_RESOURCES
              if suffix != CUSTOMER_SUFFIX]
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
                entity, record = _adapt(profile, customers, tenant, suffix, msg.value)
            except ValueError as e:
                dead.append(DeadLetter(tenant, msg.topic, msg.partition, msg.offset,
                                       msg.key, msg.value, str(e)))
                continue
            rows[entity].append(record)
        frames = {e: pd.DataFrame(recs) for e, recs in rows.items() if recs}
        if land is not None and (frames or dead):
            if frames:
                land(frames)                # raises -> offsets stay uncommitted
            if dead:
                _write_dead_letters(bootstrap, dead, topic_prefix)
            consumer.commit()
    finally:
        consumer.close()
    return ConsumeResult(frames, dead, problems)
