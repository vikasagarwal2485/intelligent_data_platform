"""Canonical record contracts (REQ-STR-03).

``config/avro/<schema>.avsc`` describes, field for field, the canonical record of
an entity as Silver's conform step produces it from the raw Bronze events (the TMF
adapter's output). The conform step checks every record against it and
quarantines the ones that break it (REQ-SLV-09). ``config/avro/silver/`` documents
Silver's dimension tables; tests check them.

The schemas are contracts only: payloads stay JSON on Kafka and Arrow/Parquet in
the lake, and the schemas aren't registered as Kafka subjects.
"""
from __future__ import annotations

import functools
import json
from typing import Any

import fastavro

from idp.common.config import DEFAULT_CONFIG_PATH

AVRO_DIR = DEFAULT_CONFIG_PATH.parent / "avro"

# Bronze entity -> its contract (voice and data usage share one)
ENTITY_SCHEMA = {
    "parties": "party",
    "customers": "customer",
    "billing_accounts": "billing_account",
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


def _describe(t: Any) -> str:
    """A field type in words, e.g. ``one of RechargeStatus`` or ``a double or null``."""
    if isinstance(t, list):
        kinds = [_describe(x) for x in t if x != "null"]
        return " or ".join(kinds) + (" or null" if "null" in t else "")
    if isinstance(t, dict):
        if t.get("type") == "enum":
            return f"one of {t['name']}"
        return _describe(t.get("type"))
    return f"a {t}"


@functools.lru_cache(maxsize=None)
def _field_checks(name: str) -> list[tuple[str, Any, str]]:
    return [(f["name"], fastavro.parse_schema(f["type"]) if not isinstance(f["type"], str) else f["type"],
             _describe(f["type"])) for f in load(name)["fields"]]


def error(entity: str, record: dict[str, Any]) -> str | None:
    """Why ``record`` breaks its entity's canonical contract, or None if it conforms.

    The record must carry exactly the contract's fields, each of its type (enum
    fields within their symbols, non-nullable fields set). The reason names the
    fields and the rule, never the offending values: it is stored in
    ``silver.quarantine``, which must not hold PII (REQ-SLV-03).
    """
    name = ENTITY_SCHEMA[entity]
    extra = set(record) - set(fields(name))
    if extra:
        return f"{name}: unexpected fields {sorted(extra)}"
    if fastavro.validation.validate(record, _parsed(name), raise_errors=False):
        return None
    bad = []
    for field, schema, expected in _field_checks(name):
        value = record.get(field)
        try:
            ok = fastavro.validation.validate(value, schema, raise_errors=False)
        except Exception:                                   # noqa: BLE001 - malformed value
            ok = False
        if not ok:
            bad.append(f"{field}: missing, expected {expected}" if value is None
                       else f"{field}: expected {expected}")
    return f"{name}: {'; '.join(bad) or 'does not conform'}"


_SPARK_TYPES = {"string": "StringType", "boolean": "BooleanType", "long": "LongType",
                "int": "IntegerType", "double": "DoubleType", "float": "FloatType"}


def spark_schema(name: str) -> Any:
    """The contract as a Spark ``StructType``, fields in order. Enums are strings, a
    union with null is nullable, and ``timestamp-micros`` longs are timestamps."""
    from pyspark.sql import types as T

    out = []
    for f in load(name)["fields"]:
        t, nullable = f["type"], False
        if isinstance(t, list):
            nullable = "null" in t
            t = next(x for x in t if x != "null")
        if isinstance(t, dict):
            if t.get("logicalType") == "timestamp-micros":
                spark_t = T.TimestampType()
            elif t.get("type") == "enum":
                spark_t = T.StringType()
            else:
                spark_t = getattr(T, _SPARK_TYPES[t["type"]])()
        else:
            spark_t = getattr(T, _SPARK_TYPES[t])()
        out.append(T.StructField(f["name"], spark_t, nullable))
    return T.StructType(out)
