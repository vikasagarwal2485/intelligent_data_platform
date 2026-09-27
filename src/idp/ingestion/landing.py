"""File landing zone (REQ-BRZ-03): a simulated source file drop, written by
``idp generate``.

Each generated entity is written as a JSON array of records to
``<landing>/tenant=<tenant_id>/<entity>.json``, one file per tenant and entity.
The landing zone is a local directory, or the bucket ``s3://<prefix>-landing`` when
S3 is enabled (``storage.landing_uri``). ``idp produce --from-landing`` publishes it
to Kafka (``read_landing`` checks each file first). The layout mirrors a
per-tenant source pickup (in production, e.g. NiFi). It doesn't write Bronze: Bronze is the Iceberg tables ``bronze.<entity>``, written by
``pipeline.to_bronze`` (from ``idp consume``, ``idp run`` and ``idp migrate-lake``).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Iterator, NamedTuple

import fsspec

from idp.common.tenancy import TENANT_COLUMN, require_tenant_column
from idp.synthetic.generator import ENTITIES, SyntheticDataset


def _fs(landing: str | Path, storage_options: dict[str, Any] | None):
    """(filesystem, root path) for a local directory or an ``s3://`` bucket."""
    return fsspec.core.url_to_fs(str(landing), **(storage_options or {}))


def write_landing(dataset: SyntheticDataset, landing: str | Path, *,
                  storage_options: dict[str, Any] | None = None) -> dict[str, list[str]]:
    """Write source records as JSON files to the landing zone (simulates NiFi pickup).

    Files are written per ``tenant=<id>/{entity}.json`` so the layout mirrors the
    per-tenant landing structure in plan 3.2. ``landing`` is a local directory or
    an ``s3://`` URI (``storage.landing_uri``). Returns the written paths per entity.
    """
    fs, root = _fs(landing, storage_options)
    written: dict[str, list[str]] = {}
    for entity in ENTITIES:
        df = getattr(dataset, entity)
        if df.empty:
            continue
        require_tenant_column(df)
        paths = []
        for tenant_id, part in df.groupby(TENANT_COLUMN):
            out_dir = f"{root.rstrip('/')}/tenant={tenant_id}"
            fs.makedirs(out_dir, exist_ok=True)
            path = f"{out_dir}/{entity}.json"
            records = part.to_dict(orient="records")
            with fs.open(path, "wb") as f:
                f.write(json.dumps(records, indent=2, default=str).encode("utf-8"))
            paths.append(path)
        written[entity] = paths
    return written


class LandingFile(NamedTuple):
    """One landing file as read for publishing: ``records`` is None when the file
    is rejected, and ``reason`` says why."""
    tenant: str
    entity: str
    path: str
    records: list[dict] | None
    reason: str | None


def read_landing(landing: str | Path, tenants: Iterable[str], *,
                 storage_options: dict[str, Any] | None = None) -> Iterator[LandingFile]:
    """Yield every landing file, checked, one at a time (REQ-STR-01).

    Tenant folders are read in sorted order and, within a folder, entities in
    ``ENTITIES`` order (the same order the in-memory producer publishes). A file is
    rejected, with ``records=None``, when its folder isn't ``tenant=<configured
    tenant>``, when it isn't ``<entity>.json`` for a known entity, when it isn't a
    JSON array of objects, or when any record's ``tenant_id`` differs from its
    folder. A rejected file is never partly returned.
    """
    fs, root = _fs(landing, storage_options)
    root = root.rstrip("/")
    if not fs.exists(root):
        return
    configured = set(tenants)
    for entry in sorted(fs.ls(root, detail=True), key=lambda e: e["name"]):
        name = entry["name"].rstrip("/").rsplit("/", 1)[-1]
        if entry["type"] != "directory" or not name.startswith("tenant="):
            yield LandingFile("", "", entry["name"], None, "not a tenant folder")
            continue
        tenant = name[len("tenant="):]
        files = {f.rsplit("/", 1)[-1]: f for f in fs.find(entry["name"])}
        known = [f"{e}.json" for e in ENTITIES if f"{e}.json" in files]
        others = sorted(set(files) - set(known))
        for fname in known + others:
            path = files[fname]
            entity = fname[:-len(".json")] if fname.endswith(".json") else fname
            if tenant not in configured:
                yield LandingFile(tenant, entity, path, None, f"unknown tenant {tenant!r}")
                continue
            if fname not in known:
                yield LandingFile(tenant, entity, path, None, f"unknown entity file {fname!r}")
                continue
            try:
                records = json.loads(fs.cat_file(path))
            except ValueError as e:
                yield LandingFile(tenant, entity, path, None, f"invalid json: {e}")
                continue
            if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
                yield LandingFile(tenant, entity, path, None, "invalid json: not an array of objects")
                continue
            mismatched = sum(1 for r in records if r.get(TENANT_COLUMN) != tenant)
            if mismatched:
                yield LandingFile(tenant, entity, path, None,
                                  f"tenant mismatch: {mismatched} record(s) not {tenant!r}")
                continue
            yield LandingFile(tenant, entity, path, records, None)
