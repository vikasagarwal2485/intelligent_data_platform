"""TM Forum mapping profiles: canonical records <-> TMF resources (REQ-BRZ-05/06).

A profile (``config/tmf/profiles/<name>.yaml``) states, for every canonical entity,
the TMF resource it becomes and where each canonical field lives. The landing writer
(:func:`to_tmf`) and the adapter (:func:`from_tmf`) both run from it, so the two
directions can't drift, and adapting what the writer produced returns the same
canonical records.

Field kinds (one per canonical field):

``path``    a standard field, by path (``usageProductPrice[0].price.dutyFreeAmount.value``)
``char``    a typed characteristic ``{@type, name, valueType, value}`` in an array
``item``    an attribute of the array entry that matches given values (contact media)
``ext``     an extension attribute (declared by the resource's ``@type`` schema)
``sum``     stored as this field plus another field (the tax-included amount),
            rounded to ``round`` decimals both ways; the way back subtracts
``const``   implied by the resource variant (e.g. ``party_type`` of an Individual)
``absent``  not carried by this resource variant: always null

Any kind may add ``map`` (canonical -> TMF values, inverted for the way back),
``as: date`` (a canonical ``YYYY-MM-DD`` carried as a TMF date-time), and ``with``
(constant TMF fields set alongside a non-null value). ``via: customer``
reads the field from the ``Customer`` (TMF629 party role, itself a mapped entity)
that the resource references with role ``customer``: the writer leaves it out (the
Customer resource carries it), and the adapter resolves the reference through the
tenant's customers (:class:`_Customers`). That reference resolution is the one
piece of structure in code.
"""
from __future__ import annotations

import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml

from idp.ingestion.tmf import schemas

PROFILE_DIR = schemas.TMF_DIR / "profiles"
CHAR_TYPES = {"string": "StringCharacteristic", "boolean": "BooleanCharacteristic",
              "integer": "IntegerCharacteristic", "number": "FloatCharacteristic"}
KINDS = ("path", "char", "item", "ext", "sum", "const", "absent")
CUSTOMER_RESOURCE = "Customer"            # TMF629: the party role ``via: customer`` fields read
_TOKEN = re.compile(r"([^.\[\]]+)|\[(\d+)\]")


class ProfileError(ValueError):
    """The profile itself is wrong (raised when it is loaded)."""


class ReferenceError_(ValueError):
    """A resource holds a reference that doesn't resolve in its tenant's files."""

    def __init__(self, file: str, message: str):
        super().__init__(message)
        self.file = file


# --------------------------------------------------------------------------- #
# paths                                                                        #
# --------------------------------------------------------------------------- #
def _keys(path: str) -> list[str | int]:
    out: list[str | int] = []
    for name, index in _TOKEN.findall(path):
        out.append(int(index) if index else name)
    return out


def get_path(obj: Any, path: str) -> Any:
    for k in _keys(path):
        if isinstance(k, int):
            if not isinstance(obj, list) or k >= len(obj):
                return None
        elif not isinstance(obj, dict):
            return None
        obj = obj[k] if isinstance(k, int) else obj.get(k)
        if obj is None:
            return None
    return obj


def set_path(obj: dict, path: str, value: Any) -> None:
    keys = _keys(path)
    for k, nxt in zip(keys, keys[1:]):
        empty = [] if isinstance(nxt, int) else {}
        if isinstance(k, int):
            while len(obj) <= k:
                obj.append([] if isinstance(nxt, int) else {})
            obj = obj[k]
        else:
            obj = obj.setdefault(k, empty)
    last = keys[-1]
    if isinstance(last, int):
        while len(obj) <= last:
            obj.append(None)
    obj[last] = value


def _template(value: Any, record: dict) -> Any:
    if isinstance(value, str) and "{" in value:
        return value.format(**{k: ("" if v is None else v) for k, v in record.items()})
    return value


# --------------------------------------------------------------------------- #
# fields                                                                       #
# --------------------------------------------------------------------------- #
@dataclass
class FieldMap:
    name: str
    kind: str
    spec: dict
    via: str | None = None
    to_tmf_values: dict = field(default_factory=dict)
    from_tmf_values: dict = field(default_factory=dict)

    @classmethod
    def parse(cls, name: str, spec: Any) -> "FieldMap":
        if spec == "absent" or spec is None:
            return cls(name, "absent", {})
        if not isinstance(spec, dict):
            raise ProfileError(f"field {name!r}: expected a mapping, got {spec!r}")
        kinds = [k for k in KINDS if k in spec]
        if len(kinds) != 1:
            raise ProfileError(f"field {name!r}: needs exactly one of {KINDS}, got {kinds or 'none'}")
        kind = kinds[0]
        if kind == "char" and spec.get("type") not in CHAR_TYPES:
            raise ProfileError(f"field {name!r}: characteristic type must be one of {list(CHAR_TYPES)}")
        if kind in ("char", "item") and "in" not in spec:
            raise ProfileError(f"field {name!r}: {kind} needs 'in' (the array)")
        via = spec.get("via")
        if via not in (None, "customer"):
            raise ProfileError(f"field {name!r}: unknown relation via={via!r}")
        fwd = dict(spec.get("map") or {})
        inv = {v: k for k, v in fwd.items()}
        if len(inv) != len(fwd):
            raise ProfileError(f"field {name!r}: map is not one-to-one, so it can't be reversed")
        return cls(name, kind, spec, via, fwd, inv)

    def forward(self, value: Any) -> Any:
        if self.spec.get("as") == "date" and isinstance(value, str) and len(value) == 10:
            return f"{value}T00:00:00Z"          # a canonical date as a TMF date-time
        return self.to_tmf_values.get(value, value) if self.to_tmf_values else value

    def back(self, value: Any) -> Any:
        if self.spec.get("as") == "date" and isinstance(value, str):
            return value[:10]
        return self.from_tmf_values.get(value, value) if self.from_tmf_values else value

    def write(self, target: dict, value: Any, record: dict) -> None:
        k, s = self.kind, self.spec
        if k in ("const", "absent"):
            return
        if k == "sum":
            base = record.get(s["of"])
            if value is None or base is None:
                return
            set_path(target, s["sum"], round(base + value, int(s.get("round", 6))))
            return
        if value is None:
            return
        v = self.forward(value)
        if k in ("path", "ext"):
            set_path(target, s[k], v)
            for extra in s.get("also", []):
                set_path(target, extra, v)
        elif k == "char":
            arr = get_path(target, s["in"])
            if arr is None:
                set_path(target, s["in"], [])
                arr = get_path(target, s["in"])
            arr.append({"@type": CHAR_TYPES[s["type"]], "name": s["char"],
                        "valueType": s["type"], "value": v})
        elif k == "item":
            arr = get_path(target, s["in"])
            if arr is None:
                set_path(target, s["in"], [])
                arr = get_path(target, s["in"])
            match = s.get("match", {})
            item = next((i for i in arr if all(i.get(mk) == mv for mk, mv in match.items())), None)
            if item is None:
                item = {**match, **(s.get("set") or {})}
                arr.append(item)
            set_path(item, s["attr"], v)
        for path, const in (s.get("with") or {}).items():
            set_path(target, path, _template(const, record))

    def read(self, source: dict | None, partial: dict) -> Any:
        k, s = self.kind, self.spec
        if k == "absent":
            return None
        if k == "const":
            return s["const"]
        if source is None:
            return None
        if k == "sum":
            total, base = get_path(source, s["sum"]), partial.get(s["of"])
            if total is None or base is None:
                return None
            return round(total - base, int(s.get("round", 6)))
        if k in ("path", "ext"):
            return self.back(get_path(source, s[k]))
        arr = get_path(source, s["in"]) or []
        if k == "char":
            item = next((i for i in arr if isinstance(i, dict) and i.get("name") == s["char"]), None)
            return None if item is None else self.back(item.get("value"))
        match = s.get("match", {})
        item = next((i for i in arr if isinstance(i, dict)
                     and all(i.get(mk) == mv for mk, mv in match.items())), None)
        return None if item is None else self.back(get_path(item, s["attr"]))


@dataclass
class ResourceMap:
    entity: str
    resource: str
    type: str
    file: str
    href: str | None
    when: dict
    constants: dict
    fields: list[FieldMap]

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    def skeleton(self, record: dict) -> dict:
        res: dict[str, Any] = {}
        for path, value in self.constants.items():
            set_path(res, path, _template(value, record))
        return res

    def selects(self, resource: dict) -> bool:
        """Whether a resource of this map's file belongs to this variant."""
        for name, value in self.when.items():
            f = next(f for f in self.fields if f.name == name)
            if f.kind in ("path", "ext") and get_path(resource, f.spec[f.kind]) != f.forward(value):
                return False
        return True


@dataclass
class Profile:
    name: str
    resources: list[ResourceMap]
    entities: list[str]

    @property
    def customer(self) -> ResourceMap:
        """The mapping of the Customer party role, which ``via: customer`` fields read."""
        found = [r for r in self.resources if r.resource == CUSTOMER_RESOURCE]
        if not found:
            raise ProfileError(f"profile {self.name!r} maps no {CUSTOMER_RESOURCE} resource")
        return found[0]

    def files(self) -> list[str]:
        return list(dict.fromkeys(r.file for r in self.resources))

    def entity_columns(self, entity: str) -> list[str]:
        return next(r for r in self.resources if r.entity == entity).field_names


def _resource_map(entry: dict, *, entity: str | None) -> ResourceMap:
    resource = entry["resource"]
    type_name = entry.get("type", f"Idp{resource}")
    constants = {"@type": type_name, "@baseType": resource}
    if type_name in schemas.extensions():
        constants["@schemaLocation"] = schemas.schema_location(type_name)
    if entry.get("href"):
        constants["href"] = entry["href"]
    constants.update(entry.get("set") or {})
    fields = [FieldMap.parse(n, s) for n, s in (entry.get("fields") or {}).items()]
    return ResourceMap(entity or "", resource, type_name, entry["file"], entry.get("href"),
                       dict(entry.get("when") or {}), constants, fields)


def parse_profile(doc: dict, name: str = "profile") -> Profile:
    try:
        resources = [_resource_map(e, entity=e["entity"]) for e in doc["resources"]]
    except KeyError as e:
        raise ProfileError(f"profile {name!r}: missing {e.args[0]!r}") from e
    if "customer" in doc:
        raise ProfileError(f"profile {name!r}: 'customer' is no longer a section; map the "
                           f"{CUSTOMER_RESOURCE} as a resource of the customers entity")
    if any(f.via for r in resources for f in r.fields) and \
            not any(r.resource == CUSTOMER_RESOURCE for r in resources):
        raise ProfileError(f"profile {name!r}: 'via: customer' fields need a {CUSTOMER_RESOURCE} resource")
    entities = list(dict.fromkeys(r.entity for r in resources))
    for ent in entities:
        variants = [r for r in resources if r.entity == ent]
        cols = variants[0].field_names
        for r in variants:
            if r.field_names != cols:
                raise ProfileError(f"profile {name!r}: the variants of {ent!r} must map the same "
                                   f"fields in the same order ({r.resource} differs)")
            if len(set(cols)) != len(cols):
                raise ProfileError(f"profile {name!r}: a field of {ent!r} is mapped twice")
            for w in r.when:
                if w not in cols:
                    raise ProfileError(f"profile {name!r}: {r.resource} 'when' uses unmapped {w!r}")
            for i, f in enumerate(r.fields):
                if f.kind == "sum" and f.spec["of"] not in cols[:i]:
                    raise ProfileError(f"profile {name!r}: {ent}.{f.name} sums {f.spec['of']!r}, "
                                       f"which must be mapped before it")
        if len(variants) > 1 and not all(r.when for r in variants):
            raise ProfileError(f"profile {name!r}: every variant of {ent!r} needs 'when'")
    return Profile(doc.get("name", name), resources, entities)


def load_profile(name: str | None = None) -> Profile:
    """The named profile (a name under ``config/tmf/profiles/`` or a ``.yaml`` path);
    by default ``IDP_TMF_PROFILE``, else ``standard`` (``cfg.tmf["profile"]``)."""
    name = name or os.environ.get("IDP_TMF_PROFILE") or "standard"
    path = Path(name) if name.endswith((".yaml", ".yml")) else PROFILE_DIR / f"{name}.yaml"
    if not path.exists():
        raise ProfileError(f"no TMF mapping profile {name!r} ({path})")
    return parse_profile(yaml.safe_load(path.read_text(encoding="utf-8")), name)


# --------------------------------------------------------------------------- #
# the Customer role (TMF629): the reference ``via: customer`` fields resolve   #
# --------------------------------------------------------------------------- #
class _Customers:
    """A tenant's Customer resources, by id: what ``via: customer`` fields read."""

    def __init__(self, profile: Profile):
        self.profile = profile
        self.by_id: dict[str, dict] = {}

    @classmethod
    def index(cls, profile: Profile, customers: list[dict]) -> "_Customers":
        idx = cls(profile)
        idx.by_id = {c.get("id"): c for c in customers}
        return idx

    def resolve(self, entity: str, resource: dict, file: str, reads_account: bool = False) -> dict:
        """The Customer the resource references with role ``customer`` (in
        ``engagedParty`` or ``relatedParty``). Raises :class:`ReferenceError_` when
        there isn't exactly one such reference, it doesn't resolve, or (with
        ``reads_account``) the customer's account is ambiguous."""
        rid = resource.get("id")
        refs = (resource.get("engagedParty") or []) + (resource.get("relatedParty") or [])
        cids = [get_path(r, "partyOrPartyRole.id") for r in refs
                if isinstance(r, dict) and r.get("role") == "customer"]
        if len(cids) != 1:
            raise ReferenceError_(file, f"{rid}: expected one customer role reference, found {len(cids)}")
        customer = self.by_id.get(cids[0])
        if customer is None:
            raise ReferenceError_(file, f"{rid}: references Customer {cids[0]!r}, which isn't in the tenant's files")
        if reads_account and len(customer.get("account") or []) != 1:
            raise ReferenceError_(file, f"{rid}: Customer {cids[0]!r} has "
                                        f"{len(customer.get('account') or [])} accounts, so its account is ambiguous")
        return customer


# --------------------------------------------------------------------------- #
# the two directions                                                           #
# --------------------------------------------------------------------------- #
def to_tmf(profile: Profile, entities: dict[str, Iterable[dict]]) -> dict[str, list[dict]]:
    """Canonical records (per entity, one tenant) -> TMF resources per landing file.

    A ``via: customer`` field isn't written: the Customer resource (written from the
    customers entity) carries it. It is checked instead: the value read back through
    the resource's customer reference must equal the record's, so an inconsistent
    dataset raises instead of silently not round-tripping.
    """
    out: dict[str, list[dict]] = defaultdict(list)
    records = {e: list(rs) for e, rs in entities.items()}
    deferred: list[tuple[ResourceMap, dict, dict]] = []      # (map, record, resource) with via fields
    for rmap in profile.resources:
        for rec in records.get(rmap.entity, []):
            if any(rec.get(k) != v for k, v in rmap.when.items()):
                continue
            res = rmap.skeleton(rec)
            for f in rmap.fields:
                value = rec.get(f.name)
                if f.via == "customer":
                    continue
                if f.kind == "absent" and value is not None:
                    raise ValueError(f"{rmap.entity} {rec.get('id')}: {rmap.resource} doesn't carry "
                                     f"{f.name}, but it is {value!r}")
                f.write(res, value, rec)
            if any(f.via for f in rmap.fields):
                deferred.append((rmap, rec, res))
            out[rmap.file].append(res)
    if deferred:
        customers = customer_index(profile, out.get(profile.customer.file, []))
        for rmap, rec, res in deferred:
            customer = customers.resolve(rmap.entity, res, rmap.file, _reads_account(rmap))
            for f in rmap.fields:
                if f.via and f.read(customer, rec) != rec.get(f.name):
                    raise ValueError(f"{rmap.entity} {rec.get('id')}: {f.name} is {rec.get(f.name)!r}, "
                                     f"but its customer {customer.get('id')!r} carries "
                                     f"{f.read(customer, rec)!r}")
    return dict(out)


def _reads_account(rmap: ResourceMap) -> bool:
    return any(f.via and str(f.spec.get("path", "")).startswith("account[") for f in rmap.fields)


def customer_index(profile: Profile, customers: Iterable[dict]) -> _Customers:
    """The index a tenant's references resolve through: its Customer resources."""
    return _Customers.index(profile, list(customers))


def resource_map(profile: Profile, resource: dict) -> ResourceMap:
    """The profile mapping a TMF resource is adapted with: by its ``@baseType`` (or
    ``@type`` for a plain TMF resource) and, for variants, their ``when``."""
    base = resource.get("@baseType") or resource.get("@type")
    maps = [r for r in profile.resources if r.resource == base and r.selects(resource)]
    if len(maps) != 1:
        raise ValueError(f"{resource.get('id')}: {len(maps)} mappings of {base!r} in profile "
                         f"{profile.name!r} match it, expected one")
    return maps[0]


def adapt_resource(profile: Profile, resource: dict, customers: _Customers,
                   rmap: ResourceMap | None = None) -> tuple[str, dict]:
    """One TMF resource -> ``(entity, canonical record)``, its references resolved
    through ``customers`` (:func:`customer_index`).

    Raises :class:`ReferenceError_` when a reference doesn't resolve.
    """
    rmap = rmap or resource_map(profile, resource)
    needs_customer = any(f.via == "customer" for f in rmap.fields)
    customer = (customers.resolve(rmap.entity, resource, rmap.file, _reads_account(rmap))
                if needs_customer else None)
    rec: dict[str, Any] = {}
    for f in rmap.fields:
        rec[f.name] = f.read(customer if f.via == "customer" else resource, rec)
    return rmap.entity, rec


def from_tmf(profile: Profile, files: dict[str, list[dict]],
             entities: Iterable[str] | None = None) -> dict[str, list[dict]]:
    """TMF resources per landing file (one tenant) -> canonical records per entity
    (all entities, or only ``entities``): :func:`adapt_resource` over the files.

    Raises :class:`ReferenceError_` (naming the file) when a reference doesn't resolve.
    """
    has_customers = any(f.via for r in profile.resources for f in r.fields)
    customers = customer_index(profile, files.get(profile.customer.file, []) if has_customers else [])
    out: dict[str, list[dict]] = {}
    wanted = list(entities) if entities is not None else profile.entities
    for ent in wanted:
        variants = [r for r in profile.resources if r.entity == ent]
        rows: list[dict] = []
        for rmap in variants:
            for res in files.get(rmap.file, []):
                if not rmap.selects(res):
                    continue
                rows.append(adapt_resource(profile, res, customers, rmap)[1])
        if len({r.file for r in variants}) > 1:
            rows.sort(key=lambda r: str(r.get("id")))        # variants from separate files
        out[ent] = rows
    return out


def entity_file_dependencies(profile: Profile) -> dict[str, set[str]]:
    """The landing files each canonical entity is adapted from (its own and the
    Customer file when a field lives on the Customer role)."""
    deps: dict[str, set[str]] = defaultdict(set)
    for r in profile.resources:
        deps[r.entity].add(r.file)
        if any(f.via == "customer" for f in r.fields):
            deps[r.entity].add(profile.customer.file)
    return dict(deps)
