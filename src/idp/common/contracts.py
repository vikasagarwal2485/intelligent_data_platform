"""Canonical record contracts (REQ-STR-03).

``config/avro/<schema>.avsc`` describes, field for field, the canonical record of a
Bronze entity: what the TMF adapter produces and Bronze stores. The consumer
checks every adapted record against it before landing (REQ-STR-02).
``config/avro/silver/`` documents Silver's tables; tests check them.

The schemas are contracts only: payloads stay JSON on Kafka and Arrow/Parquet in
the lake, and the schemas aren't registered as Kafka subjects.
"""
from __future__ import annotations

import functools
import json
from typing import Any

import fastavro
from fastavro.validation import ValidationError

from idp.common.config import DEFAULT_CONFIG_PATH

AVRO_DIR = DEFAULT_CONFIG_PATH.parent / "avro"

# Bronze entity -> its contract (voice and data usage share one)
ENTITY_SCHEMA = {
    "parties": "party",
    "accounts": "account",
    "agreements": "agreement",
    "products": "product",
    "voice_events": "usage_event",
    "data_events": "usage_event",
    "recharge_events": "recharge_event",
    "fee_charges": "fee_charge",
}


@functools.lru_cache(maxsize=None)
def load(name: str) -> dict[str, Any]:
    """A contract's Avro schema as written, e.g. ``load("party")`` or ``load("silver/dim_subscriber")``."""
    return json.loads((AVRO_DIR / f"{name}.avsc").read_text(encoding="utf-8"))


@functools.lru_cache(maxsize=None)
def _parsed(name: str) -> Any:
    return fastavro.parse_schema(load(name))


def fields(name: str) -> list[str]:
    """The contract's field names, in order."""
    return [f["name"] for f in load(name)["fields"]]


def error(entity: str, record: dict[str, Any]) -> str | None:
    """Why ``record`` breaks its entity's canonical contract, or None if it conforms.

    The record must carry exactly the contract's fields, each of its type (enum
    fields within their symbols, non-nullable fields set).
    """
    name = ENTITY_SCHEMA[entity]
    extra = set(record) - set(fields(name))
    if extra:
        return f"{name}: unexpected fields {sorted(extra)}"
    try:
        fastavro.validation.validate(record, _parsed(name), raise_errors=True)
    except ValidationError as e:
        return f"{name}: {'; '.join(str(x) for x in e.errors) or e}"
    return None
