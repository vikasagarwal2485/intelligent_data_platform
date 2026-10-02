"""Kafka producer: publish TM Forum create events to per-tenant topics (REQ-STR-01).

Each TMF resource is sent as its create event (``ingestion.tmf.events``) to
``t.<tenant>.<suffix>``, one topic per TMF resource (``streaming.TOPIC_RESOURCES``;
individuals and organizations share ``party``). A message is keyed by the
subscriber's MSISDN where the resource carries one, else by its id, so a
subscriber's events stay on one partition; customers are keyed by id, which their
log-compacted topic needs. A tenant's customers are published, and flushed, before
the resources that reference them, which the consumer resolves through them.
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import Iterable, Iterator

from idp.common.records import to_records
from idp.common.tenancy import TENANT_COLUMN
from idp.ingestion.tmf.events import base_type, to_event
from idp.ingestion.tmf.profile import Profile, load_profile, resource_map
from idp.synthetic.generator import ENTITIES, SyntheticDataset

KEY_FIELDS = ("service_id", "msisdn")          # the canonical natural key of a subscriber


def _topic(prefix: str, tenant_id: str, suffix: str) -> str:
    return f"{prefix}.{tenant_id}.{suffix}"


def message_key(profile: Profile, resource: dict) -> bytes:
    """The subscriber's MSISDN where the resource maps one (read through the
    profile), else the resource id. Customers: their id."""
    if base_type(resource) != profile.customer.resource:
        rmap = resource_map(profile, resource)
        for f in rmap.fields:
            if f.name in KEY_FIELDS and f.via != "customer":
                value = f.read(resource, {})
                if value:
                    return str(value).encode("utf-8")
    return str(resource.get("id") or "").encode("utf-8")


def tenant_events(profile: Profile, tenant: str, files: dict[str, list[dict]],
                  event_time: str | None = None) -> Iterator[tuple[str, bytes, dict]]:
    """``(topic suffix, key, event)`` for one tenant's TMF resources, in publish
    order: topics in ``TOPIC_RESOURCES`` order (customers first), each topic's
    resource types in their order, each type's resources in file order."""
    from idp.streaming import TOPIC_RESOURCES

    by_type: dict[str, list[dict]] = defaultdict(list)
    for fname in sorted(files):
        for res in files[fname]:
            by_type[base_type(res)].append(res)
    unknown = set(by_type) - {r for rs in TOPIC_RESOURCES.values() for r in rs}
    if unknown:
        raise ValueError(f"no topic for TMF resource type(s) {sorted(unknown)}")
    for suffix, resources in TOPIC_RESOURCES.items():
        for res_type in resources:
            for res in by_type.get(res_type, []):
                yield suffix, message_key(profile, res), to_event(res, tenant, event_time)


def publish_tenants(
    items: Iterable[tuple[str, dict[str, list[dict]]]],
    bootstrap: str,
    *,
    profile: Profile | None = None,
    topic_prefix: str = "t",
    acks: str | int = "all",
) -> dict[str, int]:
    """Publish ``(tenant, {landing file: TMF resources})`` as create events. Returns
    per-topic message counts.

    The single publishing path: in-memory production and publishing from the landing
    zone both come through here, so the same resources produce the same messages
    (apart from ``eventTime``, REQ-STR-01).
    """
    from kafka import KafkaProducer  # imported lazily so non-streaming use needs no dep

    profile = profile or load_profile()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=acks,
        linger_ms=50,
        retries=3,
    )
    counts: dict[str, int] = {}
    try:
        for tenant, files in items:
            customers_flushed = False
            for suffix, key, event in tenant_events(profile, tenant, files):
                if suffix != "customer" and not customers_flushed:
                    producer.flush()            # customers are on the broker first
                    customers_flushed = True
                topic = _topic(topic_prefix, tenant, suffix)
                producer.send(topic, key=key, value=event)
                counts[topic] = counts.get(topic, 0) + 1
            producer.flush()
    finally:
        producer.close()
    return counts


def dataset_files(dataset: SyntheticDataset, profile: Profile) -> Iterator[tuple[str, dict[str, list[dict]]]]:
    """A generated dataset as ``(tenant, {landing file: TMF resources})``, validated:
    exactly what ``idp generate`` writes to the landing zone."""
    from idp.ingestion.landing import tmf_files

    frames = {e: getattr(dataset, e) for e in ENTITIES}
    tenants = sorted({t for df in frames.values() if df is not None and not df.empty
                      for t in df[TENANT_COLUMN].astype(str)})
    for tenant in tenants:
        records = {e: to_records(df[df[TENANT_COLUMN].astype(str) == tenant])
                   for e, df in frames.items() if df is not None and not df.empty}
        yield tenant, tmf_files(profile, records)


def produce_to_kafka(
    dataset: SyntheticDataset,
    bootstrap: str,
    *,
    profile: Profile | None = None,
    topic_prefix: str = "t",
    acks: str | int = "all",
) -> dict[str, int]:
    """Publish a generated dataset as TMF create events. Returns per-topic message counts."""
    profile = profile or load_profile()
    return publish_tenants(dataset_files(dataset, profile), bootstrap, profile=profile,
                           topic_prefix=topic_prefix, acks=acks)
