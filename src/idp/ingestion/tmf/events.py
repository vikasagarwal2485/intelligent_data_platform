"""TMF create events: the Kafka message of a TMF resource (REQ-STR-01).

A resource travels as its ``<Resource>CreateEvent``::

    {"@type": "ProductUsageCreateEvent", "eventId": "…", "eventTime": "…Z",
     "eventType": "ProductUsageCreateEvent", "event": {"productUsage": {…}}}

``eventId`` is derived from the tenant, the resource type and its id, so the same
resource always gives the same id: in-memory and from-landing production send
equal messages. ``eventTime`` is when the event is published.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from idp.ingestion.tmf import schemas

# namespace of the platform's event ids (uuid5)
EVENT_ID_NAMESPACE = uuid.UUID("5d1f7c3e-9a0b-4c56-8e21-6f4b2a7d9c10")


class EventError(ValueError):
    """A message that isn't a valid create event of its topic."""


def base_type(resource: dict) -> str:
    """The TMF resource a resource is (its ``@baseType``, or ``@type`` when plain)."""
    return resource.get("@baseType") or resource.get("@type") or ""


def event_id(tenant: str, resource: dict) -> str:
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, f"{tenant}/{base_type(resource)}/{resource.get('id')}"))


def now() -> str:
    """An ``eventTime``: the current UTC time, ISO-8601 with milliseconds."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def to_event(resource: dict, tenant: str, event_time: str | None = None) -> dict:
    """Wrap a TMF resource in its create event."""
    base = base_type(resource)
    name = schemas.event_type(base)
    return {"@type": name, "eventId": event_id(tenant, resource), "eventTime": event_time or now(),
            "eventType": name, "event": {schemas.payload_name(base): resource}}


def from_event(value: Any, resources: Iterable[str], tenant: str) -> dict:
    """The resource of a create event read from a topic that carries ``resources``,
    for ``tenant``. Checks, in order: the event type belongs to the topic, the
    envelope, the resource's schema and extensions (REQ-BRZ-06), its ``tenantId``.

    Raises :class:`EventError` with the reason.
    """
    allowed = {schemas.event_type(r): r for r in resources}
    etype = value.get("eventType") if isinstance(value, dict) else None
    if etype not in allowed:
        raise EventError(f"eventType {etype!r} isn't a create event of this topic "
                         f"({', '.join(allowed)})")
    res = allowed[etype]
    err = next(schemas.event_errors(value, res), None)
    if err:
        raise EventError(f"invalid event: {err}")
    resource = value["event"][schemas.payload_name(res)]
    if base_type(resource) != res:
        raise EventError(f"{etype} carries a {base_type(resource)!r}, not a {res!r}")
    err = schemas.first_error(resource, res)
    if err:
        raise EventError(f"invalid {res} {resource.get('id')!r}: {err}")
    if resource.get("tenantId") != tenant:
        raise EventError(f"{res} {resource.get('id')!r} has tenantId {resource.get('tenantId')!r}, "
                         f"not the topic's tenant {tenant!r}")
    return resource
