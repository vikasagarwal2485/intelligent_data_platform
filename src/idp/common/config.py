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
        s = dict(self.raw.get("synthetic", {}))
        env = os.environ
        if "IDP_SUBSCRIBERS" in env:
            s["subscribers_per_tenant"] = int(env["IDP_SUBSCRIBERS"])
        if "IDP_DAYS" in env:
            s["days"] = int(env["IDP_DAYS"])
        if "IDP_FRAUD_RATE" in env:
            s["fraud_rate"] = float(env["IDP_FRAUD_RATE"])
        return s

    @property
    def features(self) -> dict[str, Any]:
        return self.raw.get("features", {})

    @property
    def credit(self) -> dict[str, Any]:
        return self.raw.get("credit", {})

    @property
    def ml(self) -> dict[str, Any]:
        """Offline training bounds (REQ-ML-09)."""
        return {"train_max_rows": 2_000_000, "sample_seed": 42, **self.raw.get("ml", {})}

    @property
    def fraud(self) -> dict[str, Any]:
        return self.raw.get("fraud", {})

    @property
    def auth(self) -> dict[str, Any]:
        """API authentication (REQ-SRV-03): Keycloak-issued Bearer JWTs, with the
        caller's tenant from a token claim. Off by default (local dev and tests);
        the cluster overlays turn it on with IDP_AUTH_ENABLED=true."""
        a = {"enabled": False, "issuer": "", "audience": "idp-scoring-api",
             "tenant_claim": "tenant_id", "leeway_seconds": 30, **self.raw.get("auth", {})}
        env = os.environ
        if "IDP_AUTH_ENABLED" in env:
            a["enabled"] = env["IDP_AUTH_ENABLED"].strip().lower() in ("1", "true", "yes", "on")
        a["enabled"] = bool(a["enabled"])
        a["issuer"] = env.get("IDP_AUTH_ISSUER", a["issuer"] or "")
        a["audience"] = env.get("IDP_AUTH_AUDIENCE", a["audience"])
        a["tenant_claim"] = env.get("IDP_AUTH_TENANT_CLAIM", a["tenant_claim"])
        a["leeway_seconds"] = int(env.get("IDP_AUTH_LEEWAY_SECONDS", a["leeway_seconds"]))
        return a

    @property
    def mlflow(self) -> dict[str, Any]:
        """Model registry (REQ-ML-04): MLflow when ``tracking_uri`` is set, else the
        object-store fallback."""
        m = {"tracking_uri": "", "refresh_seconds": 60, "experiment": "idp-training",
             **self.raw.get("mlflow", {})}
        m["tracking_uri"] = os.environ.get("IDP_MLFLOW_TRACKING_URI", m["tracking_uri"] or "")
        m["refresh_seconds"] = float(os.environ.get("IDP_MLFLOW_REFRESH_SECONDS", m["refresh_seconds"]))
        return m

    @property
    def kafka(self) -> dict[str, Any]:
        k = dict(self.raw.get("kafka", {}))
        k["bootstrap"] = os.environ.get("IDP_KAFKA_BOOTSTRAP", k.get("bootstrap", "localhost:9092"))
        k["schema_registry"] = os.environ.get("IDP_SCHEMA_REGISTRY", k.get("schema_registry", ""))
        k.setdefault("topic_prefix", "t")
        return k

    @property
    def trino(self) -> dict[str, Any]:
        """SQL query engine for the analytics tools (REQ-MCP-06): Trino's lake catalog."""
        t = dict(self.raw.get("trino", {}))
        env = os.environ
        t["host"] = env.get("IDP_TRINO_HOST", t.get("host", "127.0.0.1"))
        t["port"] = int(env.get("IDP_TRINO_PORT", t.get("port", 8085)))
        t["user"] = env.get("IDP_TRINO_USER", t.get("user", "idp-mcp"))
        t["catalog"] = env.get("IDP_TRINO_CATALOG", t.get("catalog", "lake"))
        t.setdefault("http_scheme", "http")
        return t

    @property
    def timescale(self) -> dict[str, Any]:
        t = dict(self.raw.get("timescale", {}))
        env = os.environ
        if "IDP_TS_HOST" in env:
            t["enabled"] = True
            t["host"] = env["IDP_TS_HOST"]
        t["port"] = int(env.get("IDP_TS_PORT", t.get("port", 5432)))
        t["database"] = env.get("IDP_TS_DB", t.get("database", "idp"))
        t["user"] = env.get("IDP_TS_USER", t.get("user", "postgres"))
        t["password"] = env.get("IDP_TS_PASSWORD", t.get("password", ""))
        t.setdefault("host", "timescaledb.idp-platform.svc")
        t.setdefault("schema", "gold")
        t.setdefault("enabled", False)
        return t

    @property
    def catalog(self) -> dict[str, Any]:
        """Iceberg catalog for the lake (REQ-FND-04): JDBC on SQLite (local) or Postgres (cluster)."""
        c = dict(self.raw.get("catalog", {}))
        env = os.environ
        c["type"] = env.get("IDP_CATALOG_TYPE", c.get("type") or "sql").lower()
        c["uri"] = env.get("IDP_CATALOG_URI", c.get("uri") or "")
        c.setdefault("warehouse", "")
        c.setdefault("branch", "main")
        root = Path(self.storage.root).resolve()
        c["name"] = env.get("IDP_CATALOG_NAME", c.get("name") or "")
        if c["type"] == "sql":
            c["uri"] = c["uri"] or f"sqlite:///{root / 'catalog.db'}"
            # only the default for tables created without a location; every lake
            # table is created in its own layer (catalog.table_location)
            s3 = self.s3
            default = f"s3://{s3['bucket_prefix']}-bronze" if s3.get("enabled") else root.as_uri()
            c["warehouse"] = c["warehouse"] or default
        return c

    @property
    def lake(self) -> dict[str, Any]:
        """Lake maintenance and cost settings (REQ-ORC-06)."""
        defaults = {"snapshot_retention_days": 7, "bronze_archive_days": 90, "target_file_size_mb": 128}
        return {**defaults, **self.raw.get("lake", {})}

    @property
    def spark(self) -> dict[str, Any]:
        """Spark runtime settings for the Silver/Gold stages (REQ-ORC-05)."""
        s = dict(self.raw.get("spark", {}))
        env = os.environ
        s["master"] = env.get("IDP_SPARK_MASTER", s.get("master", "local[*]"))
        s["driver_memory"] = env.get("IDP_SPARK_DRIVER_MEMORY", s.get("driver_memory", "1g"))
        return s

    def spark_profile(self, stage: str) -> dict[str, dict[str, Any]]:
        """Resource profile of a Spark stage (REQ-ORC-07): ``{"driver": {...},
        "executor": {...}}`` with ``cores``, ``memory``, ``memoryOverhead`` (and the
        executor's ``instances``). Unknown stages get an empty profile."""
        prof = dict(self.raw.get("spark", {}).get("profiles", {}).get(stage, {}))
        return {"driver": dict(prof.get("driver", {})), "executor": dict(prof.get("executor", {}))}

    @property
    def s3(self) -> dict[str, Any]:
        s = dict(self.raw.get("s3", {}))
        env = os.environ
        if "IDP_S3_ENDPOINT" in env:
            s["enabled"] = True
            s["endpoint"] = env["IDP_S3_ENDPOINT"]
        s["access_key"] = env.get("IDP_S3_ACCESS_KEY", s.get("access_key", ""))
        s["secret_key"] = env.get("IDP_S3_SECRET_KEY", s.get("secret_key", ""))
        # One bucket per medallion layer: <prefix>-bronze / -silver / -gold.
        s["bucket_prefix"] = env.get("IDP_S3_BUCKET_PREFIX", s.get("bucket_prefix", "idp"))
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
