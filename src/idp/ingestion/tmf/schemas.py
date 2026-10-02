"""Validation of TM Forum landing resources (REQ-BRZ-06).

The TMF schemas are vendored as JSON Schema bundles in ``config/tmf/schemas/`` (see
``config/tmf/vendor_schemas.py``), and the extension schemas live in
``config/tmf/extensions/`` (the platform's ``Idp<Resource>`` and any customer
subtypes). Nothing is fetched at run time: a ``$ref`` outside these files is an error.

A resource is validated against the schema of its ``@type``:
- an extension type (it has a schema in ``extensions/``) against that schema, which
  is ``allOf`` its base resource (or another extension) and its own attributes;
- a plain TMF type against its definition in the bundle of its API.

For speed (landing files hold hundreds of thousands of usage resources), each type's
schema is merged with everything it references into one self-contained schema and
compiled once with ``fastjsonschema`` (about 10k resources a second; the
interpreted ``jsonschema`` reaches under 1k).
"""
from __future__ import annotations

import functools
import json
from pathlib import Path
from typing import Any, Callable, Iterator

import fastjsonschema

from idp.common.config import DEFAULT_CONFIG_PATH

TMF_DIR = DEFAULT_CONFIG_PATH.parent / "tmf"
SCHEMA_DIR = TMF_DIR / "schemas"
EXTENSION_DIR = TMF_DIR / "extensions"
EXTENSION_BASE_URI = "https://idp.local/tmf/extensions/"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


@functools.lru_cache(maxsize=None)
def bundles() -> dict[str, dict[str, Any]]:
    """The vendored TMF bundles, by file name."""
    return {p.name: _load(p) for p in sorted(SCHEMA_DIR.glob("*.schema.json"))}


@functools.lru_cache(maxsize=None)
def extensions() -> dict[str, dict[str, Any]]:
    """The extension schemas, by the ``@type`` they define (their file stem)."""
    return {p.name.removesuffix(".schema.json"): _load(p)
            for p in sorted(EXTENSION_DIR.glob("*.schema.json"))}


@functools.lru_cache(maxsize=None)
def resource_bundle() -> dict[str, str]:
    """TMF resource name -> the bundle that defines it (from each bundle's ``x-resources``)."""
    out: dict[str, str] = {}
    for name, b in bundles().items():
        for res in b.get("x-resources", []):
            out.setdefault(res, name)
    return out


def schema_location(type_name: str) -> str:
    """The ``@schemaLocation`` an extension type is emitted with."""
    return f"{EXTENSION_BASE_URI}{type_name}.schema.json"


_META = {"$id", "$schema", "title", "description", "x-source", "x-resources",
         "x-baseType", "x-baseSchema"}


def _documents() -> list[dict[str, Any]]:
    return list(bundles().values()) + list(extensions().values())


def _key(doc_id: str, name: str) -> str:
    return f"{doc_id.rsplit('/', 1)[-1].removesuffix('.schema.json')}__{name}"


def _rewrite(node: Any, base_id: str, by_file: dict[str, str]) -> Any:
    """Point every ``$ref`` at the merged ``definitions`` (JSON pointers only)."""
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k in _META:
                continue
            if k == "$ref":
                uri, _, frag = v.partition("#")
                target = base_id
                if uri:
                    fname = uri.rsplit("/", 1)[-1]
                    if fname not in by_file:
                        raise KeyError(f"unresolvable $ref {v!r} in {base_id}")
                    target = by_file[fname]
                name = frag[len("/$defs/"):] if frag.startswith("/$defs/") else "__root__"
                out[k] = "#/definitions/" + _key(target, name)
            else:
                out[k] = _rewrite(v, base_id, by_file)
        return out
    if isinstance(node, list):
        return [_rewrite(v, base_id, by_file) for v in node]
    return node


@functools.lru_cache(maxsize=None)
def merged_definitions() -> dict[str, Any]:
    """Every vendored definition and extension schema, in one ``definitions`` map."""
    docs = _documents()
    by_file = {d["$id"].rsplit("/", 1)[-1]: d["$id"] for d in docs}
    defs: dict[str, Any] = {}
    for doc in docs:
        for name, d in doc.get("$defs", {}).items():
            defs[_key(doc["$id"], name)] = _rewrite(d, doc["$id"], by_file)
        if "allOf" in doc:
            defs[_key(doc["$id"], "__root__")] = _rewrite({"allOf": doc["allOf"]}, doc["$id"], by_file)
    missing = {ref.split("/")[-1] for ref in _refs(defs)} - set(defs)
    if missing:
        raise KeyError(f"unresolvable $ref targets: {sorted(missing)[:5]}")
    return defs


def _refs(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        if isinstance(node.get("$ref"), str):
            yield node["$ref"]
        for v in node.values():
            yield from _refs(v)
    elif isinstance(node, list):
        for v in node:
            yield from _refs(v)


@functools.lru_cache(maxsize=None)
def validator(type_name: str, base_type: str | None = None) -> Callable[[Any], Any]:
    """The compiled validator for resources of ``@type`` ``type_name`` (``base_type``
    is the TMF resource, needed when ``type_name`` is a plain TMF type)."""
    if type_name in extensions():
        root = _key(extensions()[type_name]["$id"], "__root__")
    else:
        base = base_type or type_name
        bundle = resource_bundle().get(base)
        if bundle is None:
            raise KeyError(f"no vendored TMF schema for {base!r}")
        root = _key(bundles()[bundle]["$id"], base)
    schema = {"$schema": "http://json-schema.org/draft-07/schema#",
              "definitions": merged_definitions(), "$ref": f"#/definitions/{root}"}
    return fastjsonschema.compile(schema)


def errors(resource: dict[str, Any], base_type: str | None = None) -> Iterator[str]:
    """The validation error of one resource, if any, as ``path: message``."""
    if not isinstance(resource, dict):
        yield f"$: not an object ({type(resource).__name__})"
        return
    type_name = resource.get("@type")
    if not type_name:
        yield "$: missing @type"
        return
    try:
        check = validator(type_name, resource.get("@baseType") or base_type)
    except KeyError as e:
        yield f"$: {e.args[0]}"
        return
    try:
        check(resource)
    except fastjsonschema.JsonSchemaException as e:
        path = (e.name or "data").replace("data", "$", 1)
        yield f"{path}: {e.message.replace(e.name or 'data', '', 1).strip() or e.message}"


def first_error(resource: dict[str, Any], base_type: str | None = None) -> str | None:
    return next(errors(resource, base_type), None)
