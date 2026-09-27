"""Kafka consumer: drain topics into Bronze frames (plan 3.2/3.3).

Batch-drains all `t.<tenant>.<entity>` topics (stops after `idle_timeout`
seconds with no new records) and returns the raw records grouped back into the
generator's table layout, ready for the Silver curation step.
"""
from __future__ import annotations

import json
from typing import Any, Callable

import pandas as pd


def consume_to_bronze(
    bootstrap: str,
    tenants: list[str],
    *,
    topic_prefix: str = "t",
    idle_timeout: float = 15.0,
    group_id: str = "idp-bronze-loader",
    land: Callable[[dict[str, pd.DataFrame]], Any] | None = None,
) -> dict[str, pd.DataFrame]:
    """Consume all entity/event topics and return {table_name: DataFrame}.

    With ``land`` (e.g. ``lambda frames: to_bronze(cfg, frames)``), the drained
    batch is handed to it and the Kafka offsets are committed ONLY after it
    returns: if landing fails, nothing is committed and the next consume re-reads
    the batch (Bronze duplicates are harmless; Silver merges on natural keys).
    Without ``land`` nothing is committed.
    """
    from kafka import KafkaConsumer
    from idp.streaming import SUFFIX_TO_TABLE

    topics = [
        f"{topic_prefix}.{t}.{suffix}"
        for t in tenants
        for suffix in SUFFIX_TO_TABLE
    ]
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,           # commit only after Bronze is written
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=int(idle_timeout * 1000),
    )
    rows: dict[str, list[dict]] = {t: [] for t in set(SUFFIX_TO_TABLE.values())}
    try:
        for msg in consumer:  # ends after idle_timeout with no messages
            # topic format: <prefix>.<tenant>.<suffix...>
            suffix = msg.topic.split(".", 2)[2]
            table = SUFFIX_TO_TABLE.get(suffix)
            if table:
                rows[table].append(msg.value)
        frames = {table: pd.DataFrame(recs) for table, recs in rows.items() if recs}
        if land is not None and frames:
            land(frames)                    # raises -> offsets stay uncommitted
            consumer.commit()
    finally:
        consumer.close()
    return frames
