"""Configuration loading for the platform.

Loads ``config/platform.yaml`` into typed objects. Keeping this small and
dependency-light (PyYAML + dataclasses) so it is trivial to use from CLI,
pipelines and tests.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "platform.yaml"


@dataclass(frozen=True)
class Tenant:
    id: str
    name: str
    region: str
    pii_salt: str


@dataclass(frozen=True)
class StorageConfig:
    root: str
    landing: str
    bronze: str
    silver: str
    gold: str
    ml_artifacts: str


@dataclass
class PlatformConfig:
    raw: dict[str, Any]
    tenants: list[Tenant]
    storage: StorageConfig

    # --- convenience accessors -------------------------------------------------
    @property
    def tenant_ids(self) -> list[str]:
        return [t.id for t in self.tenants]

    def tenant(self, tenant_id: str) -> Tenant:
        for t in self.tenants:
            if t.id == tenant_id:
                return t
        raise KeyError(f"Unknown tenant_id: {tenant_id!r}")

    @property
    def synthetic(self) -> dict[str, Any]:
        return self.raw.get("synthetic", {})

    @property
    def features(self) -> dict[str, Any]:
        return self.raw.get("features", {})

    @property
    def credit(self) -> dict[str, Any]:
        return self.raw.get("credit", {})

    @property
    def fraud(self) -> dict[str, Any]:
        return self.raw.get("fraud", {})

    @property
    def kafka(self) -> dict[str, Any]:
        k = dict(self.raw.get("kafka", {}))
        k["bootstrap"] = os.environ.get("IDP_KAFKA_BOOTSTRAP", k.get("bootstrap", "localhost:9092"))
        k["schema_registry"] = os.environ.get("IDP_SCHEMA_REGISTRY", k.get("schema_registry", ""))
        k.setdefault("topic_prefix", "t")
        return k

    @property
    def s3(self) -> dict[str, Any]:
        s = dict(self.raw.get("s3", {}))
        env = os.environ
        if "IDP_S3_ENDPOINT" in env:
            s["enabled"] = True
            s["endpoint"] = env["IDP_S3_ENDPOINT"]
        s["access_key"] = env.get("IDP_S3_ACCESS_KEY", s.get("access_key", ""))
        s["secret_key"] = env.get("IDP_S3_SECRET_KEY", s.get("secret_key", ""))
        s["bucket"] = env.get("IDP_S3_BUCKET", s.get("bucket", "idp-bronze"))
        s.setdefault("enabled", False)
        return s


def load_config(path: str | os.PathLike | None = None) -> PlatformConfig:
    """Load platform configuration from YAML."""
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    tenants = [Tenant(**t) for t in raw.get("tenants", [])]
    storage = StorageConfig(**raw["storage"])
    return PlatformConfig(raw=raw, tenants=tenants, storage=storage)
