"""File landing zone (REQ-BRZ-03): a simulated source file drop of TM Forum
Open API resources, written by ``idp generate``.

Each tenant's data is written as TMF v5 resources, one JSON array per resource type,
to ``<landing>/tenant=<tenant_id>/<resource>.json`` (``individual.json``,
``customer.json``, ``productUsage.json`` ...). The canonical records are mapped to
the resources by the active mapping profile (``idp.ingestion.tmf``). The landing
zone is a local directory, or the bucket ``s3://<prefix>-landing`` when S3 is
enabled (``storage.landing_uri``).

``idp produce --from-landing`` reads it back through :func:`read_landing_resources`,
which checks the files and yields their resources to publish as TMF events
(REQ-STR-01); the consumer adapts them to canonical records (REQ-BRZ-05).
:func:`read_landing` adapts a whole landing zone to canonical records directly. The
layout mirrors a per-tenant source pickup (in production, e.g. NiFi). It doesn't
write Bronze: Bronze is the Iceberg tables ``bronze.<entity>``, written by
``pipeline.to_bronze`` (from ``idp consume`` and ``idp run``).
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator, NamedTuple

import fsspec

from idp.common.records import to_records
from idp.common.tenancy import TENANT_COLUMN, require_tenant_column
from idp.ingestion.tmf import schemas
from idp.ingestion.tmf.profile import (Profile, ReferenceError_, entity_file_dependencies,
                                       from_tmf, load_profile, to_tmf)
from idp.synthetic.generator import ENTITIES, SyntheticDataset

TENANT_ATTRIBUTE = "tenantId"          # the platform extension attribute on every resource


def _fs(landing: str | Path, storage_options: dict[str, Any] | None):
    """(filesystem, root path) for a local directory or an ``s3://`` bucket."""
    return fsspec.core.url_to_fs(str(landing), **(storage_options or {}))


def tmf_files(profile: Profile, records: dict[str, Iterable[dict]]) -> dict[str, list[dict]]:
    """One tenant's canonical records as TMF resources per landing file, each
    validated: the resources ``idp generate`` writes and ``idp produce`` publishes
    from memory. An invalid resource is a bug in the profile, so it raises."""
    files = to_tmf(profile, records)
    for fname, resources in files.items():
        for resource in resources:
            err = schemas.first_error(resource)
            if err:
                raise ValueError(f"{fname} {resource.get('id')}: invalid TMF resource ({err})")
    return files


def write_landing(dataset: SyntheticDataset, landing: str | Path, *,
                  storage_options: dict[str, Any] | None = None,
                  profile: Profile | None = None) -> dict[str, list[str]]:
    """Write the dataset as TMF resource files to the landing zone, per tenant.

    Every resource is validated against its schema before it is written: an invalid
    resource is a bug in the profile, so it raises rather than landing. ``landing``
    is a local directory or an ``s3://`` URI. Returns the written paths per file name.
    """
    profile = profile or load_profile()
    fs, root = _fs(landing, storage_options)
    frames = {e: getattr(dataset, e) for e in ENTITIES}
    tenants: set[str] = set()
    for df in frames.values():
        if df is not None and not df.empty:
            require_tenant_column(df)
            tenants |= set(df[TENANT_COLUMN].astype(str))
    written: dict[str, list[str]] = defaultdict(list)
    for tenant_id in sorted(tenants):
        records = {e: to_records(df[df[TENANT_COLUMN].astype(str) == tenant_id])
                   for e, df in frames.items() if df is not None and not df.empty}
        out_dir = f"{root.rstrip('/')}/tenant={tenant_id}"
        fs.makedirs(out_dir, exist_ok=True)
        for fname, resources in tmf_files(profile, records).items():
            path = f"{out_dir}/{fname}"
            with fs.open(path, "wb") as f:
                f.write(json.dumps(resources, indent=2).encode("utf-8"))
            written[fname].append(path)
    return dict(written)


class LandingFile(NamedTuple):
    """One canonical entity of a tenant, as read for publishing (``entity`` names
    it; ``path`` is the landing file(s) it was adapted from). ``records`` is None
    when it is rejected, and ``reason`` says why. A rejected landing file is also
    reported on its own, under the file's stem."""
    tenant: str
    entity: str
    path: str
    records: list[dict] | None
    reason: str | None


def _check_file(raw: bytes, tenant: str) -> tuple[list[dict] | None, str | None]:
    """The resources of one landing file, or why it is rejected."""
    try:
        resources = json.loads(raw)
    except ValueError as e:
        return None, f"invalid json: {e}"
    if not isinstance(resources, list) or not all(isinstance(r, dict) for r in resources):
        return None, "invalid json: not an array of objects"
    mismatched = sum(1 for r in resources if r.get(TENANT_ATTRIBUTE) != tenant)
    if mismatched:
        return None, f"tenant mismatch: {mismatched} resource(s) not {tenant!r}"
    for r in resources:
        err = schemas.first_error(r)
        if err:
            return None, f"invalid resource {r.get('id')!r}: {err}"
    return resources, None


class _Folder(NamedTuple):
    """One entry of the landing zone, checked: a tenant folder, or a stray entry."""
    tenant: str
    path: str
    files: dict[str, str]                 # file name -> path
    loaded: dict[str, list[dict]]         # file name -> resources, for files that passed
    rejected: dict[str, str]              # file name -> why it was rejected


def _folders(landing: str | Path, tenants: Iterable[str], storage_options: dict[str, Any] | None,
             profile: Profile) -> Iterator[_Folder]:
    """Every entry of the landing zone in sorted order, its files checked one by one.
    A stray entry (not a ``tenant=`` folder) comes with no tenant."""
    known = set(profile.files())
    fs, root = _fs(landing, storage_options)
    root = root.rstrip("/")
    if not fs.exists(root):
        return
    configured = set(tenants)
    for entry in sorted(fs.ls(root, detail=True), key=lambda e: e["name"]):
        name = entry["name"].rstrip("/").rsplit("/", 1)[-1]
        if entry["type"] != "directory" or not name.startswith("tenant="):
            yield _Folder("", entry["name"], {}, {}, {})
            continue
        tenant = name[len("tenant="):]
        files = {f.rsplit("/", 1)[-1]: f for f in fs.find(entry["name"])}
        if tenant not in configured:
            yield _Folder(tenant, entry["name"], files, {},
                          {f: f"unknown tenant {tenant!r}" for f in sorted(files)})
            continue
        loaded: dict[str, list[dict]] = {}
        rejected: dict[str, str] = {}
        for fname in sorted(files):
            if fname not in known:
                rejected[fname] = f"unknown landing file {fname!r}"
                continue
            resources, reason = _check_file(fs.cat_file(files[fname]), tenant)
            if reason:
                rejected[fname] = reason
            else:
                loaded[fname] = resources
        yield _Folder(tenant, entry["name"], files, loaded, rejected)


def _entities(profile: Profile, folder: _Folder) -> Iterator[tuple[str, list[str], list[dict] | None,
                                                                 str | None, str | None]]:
    """A tenant folder's canonical entities, in profile order: ``(entity, its own
    files, records, reason, file to blame)``. An entity is rejected when a file it is
    adapted from was rejected, or when one of its references doesn't resolve."""
    needs = entity_file_dependencies(profile)
    for entity in profile.entities:
        own = [f for f in dict.fromkeys(r.file for r in profile.resources if r.entity == entity)
               if f in folder.files]
        if not own:
            continue                                   # nothing landed for this entity
        broken = sorted(f for f in needs[entity] if f in folder.rejected)
        if broken:
            yield entity, own, None, f"depends on rejected file {broken[0]!r}", None
            continue
        try:
            records = from_tmf(profile, folder.loaded, entities=[entity])[entity]
        except ReferenceError_ as e:
            yield entity, own, None, f"unresolved reference in {e.file!r}: {e}", e.file
            continue
        yield entity, own, records, None, None


def read_landing(landing: str | Path, tenants: Iterable[str], *,
                 storage_options: dict[str, Any] | None = None,
                 profile: Profile | None = None) -> Iterator[LandingFile]:
    """Yield every tenant's canonical entities from the landing zone, checked and
    adapted, one entity at a time (REQ-BRZ-05).

    Tenant folders are read in sorted order, and each folder's entities in
    ``ENTITIES`` order. A landing file is rejected (and reported) when its folder
    isn't ``tenant=<configured tenant>``, when it isn't a file of the mapping
    profile, when it isn't a JSON array of resources that validate against their
    schemas, or when any resource's ``tenantId`` differs from its folder. An entity
    is rejected when a file it is adapted from was rejected, or when one of its
    references doesn't resolve. A rejected entity is never partly returned.
    """
    profile = profile or load_profile()
    configured = set(tenants)
    for folder in _folders(landing, configured, storage_options, profile):
        if not folder.tenant:
            yield LandingFile("", "", folder.path, None, "not a tenant folder")
            continue
        for fname, reason in folder.rejected.items():
            yield LandingFile(folder.tenant, fname.removesuffix(".json"), folder.files[fname], None, reason)
        if folder.tenant in configured:
            for entity, own, records, reason, _ in _entities(profile, folder):
                yield LandingFile(folder.tenant, entity, ",".join(folder.files[f] for f in own),
                                  records, reason)


class LandingResources(NamedTuple):
    """One landing file of a tenant, as read for publishing (REQ-STR-01):
    ``resources`` is None when it is rejected, and ``reason`` says why."""
    tenant: str
    file: str
    path: str
    resources: list[dict] | None
    reason: str | None


def read_landing_resources(landing: str | Path, tenants: Iterable[str], *,
                           storage_options: dict[str, Any] | None = None,
                           profile: Profile | None = None) -> Iterator[LandingResources]:
    """Yield every tenant's landing files, checked, with their TMF resources as they
    are (to publish as events), tenant folders in sorted order.

    A file is rejected for the reasons :func:`read_landing` gives: its own checks
    (folder tenant, known file, schema, ``tenantId``), a rejected file it depends on
    (e.g. ``customer.json`` for the files referencing customers), or a resource whose
    reference doesn't resolve. To find the last two, the tenant's files are adapted
    once and the canonical records discarded, so the rules and messages are exactly
    those of :func:`read_landing`.
    """
    profile = profile or load_profile()
    configured = set(tenants)
    for folder in _folders(landing, configured, storage_options, profile):
        if not folder.tenant:
            yield LandingResources("", "", folder.path, None, "not a tenant folder")
            continue
        reasons = dict(folder.rejected)
        if folder.tenant in configured:
            for _, own, _, reason, blame in _entities(profile, folder):
                if reason:
                    for fname in ([blame] if blame else own):
                        reasons.setdefault(fname, reason)
        for fname in sorted(folder.files):
            if fname in reasons:
                yield LandingResources(folder.tenant, fname, folder.files[fname], None, reasons[fname])
            else:
                yield LandingResources(folder.tenant, fname, folder.files[fname],
                                       folder.loaded[fname], None)
