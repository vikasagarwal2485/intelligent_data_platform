"""Configuration loading and env overrides (REQ-FND-01)."""
from __future__ import annotations

from idp.common.config import load_config


def test_config_exposes_all_sections():
    # SCN-FND-01.1
    cfg = load_config()
    for section in ("tenants", "storage", "kafka", "s3", "catalog", "lake", "trino", "mcp", "mlflow", "auth", "timescale",
                    "spark", "synthetic", "features", "credit", "fraud"):
        assert getattr(cfg, section) is not None, section


def test_spark_defaults(monkeypatch):
    monkeypatch.delenv("IDP_SPARK_MASTER", raising=False)
    monkeypatch.delenv("IDP_SPARK_DRIVER_MEMORY", raising=False)
    spark = load_config().spark
    assert spark["master"] == "local[*]"
    assert spark["driver_memory"] == "1g"


def test_spark_env_overrides(monkeypatch):
    # SCN-FND-01.4
    monkeypatch.setenv("IDP_SPARK_MASTER", "spark://master:7077")
    monkeypatch.setenv("IDP_SPARK_DRIVER_MEMORY", "2g")
    spark = load_config().spark
    assert spark == {**spark, "master": "spark://master:7077", "driver_memory": "2g"}


def test_s3_enabled_via_env(monkeypatch):
    # SCN-FND-01.2
    monkeypatch.setenv("IDP_S3_ENDPOINT", "http://minio:9000")
    s3 = load_config().s3
    assert s3["enabled"] is True and s3["endpoint"] == "http://minio:9000"


def test_catalog_defaults_to_local_sqlite(monkeypatch):
    # SCN-FND-01.5 — no env: the local file-based catalog under the lake root
    monkeypatch.delenv("IDP_CATALOG_TYPE", raising=False)
    monkeypatch.delenv("IDP_CATALOG_URI", raising=False)
    from pathlib import Path
    cfg = load_config()
    cat = cfg.catalog
    root = Path(cfg.storage.root).resolve()
    assert cat["type"] == "sql" and cat["uri"] == f"sqlite:///{root / 'catalog.db'}"
    assert cat["warehouse"] == root.as_uri()
    assert cfg.lake["snapshot_retention_days"] == 7


def test_catalog_postgres_via_env(monkeypatch):
    # SCN-FND-01.5 — cluster: the Iceberg JDBC catalog on the platform Postgres
    uri = "postgresql+psycopg2://postgres:pw@timescaledb.idp-platform.svc:5432/iceberg"
    monkeypatch.setenv("IDP_CATALOG_TYPE", "sql")
    monkeypatch.setenv("IDP_CATALOG_URI", uri)
    monkeypatch.setenv("IDP_S3_ENDPOINT", "http://minio.idp-platform.svc:9000")
    cfg = load_config()
    assert cfg.catalog["type"] == "sql" and cfg.catalog["uri"] == uri
    assert cfg.catalog["warehouse"] == f"s3://{cfg.s3['bucket_prefix']}-bronze"
    from idp.common import catalog
    conf = catalog.spark_catalog_conf(cfg)
    p = f"spark.sql.catalog.{catalog.catalog_name(cfg)}"
    assert conf[f"{p}.type"] == "jdbc"
    assert conf[f"{p}.uri"] == "jdbc:postgresql://timescaledb.idp-platform.svc:5432/iceberg"
    assert conf[f"{p}.jdbc.user"] == "postgres" and conf[f"{p}.jdbc.password"] == "pw"
    assert conf[f"{p}.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert "connect_args.timeout" not in catalog.pyiceberg_properties(cfg)   # SQLite only

def test_catalog_name_is_stable_for_shared_catalogs(monkeypatch):
    # SCN-FND-01.7 — `lake` for the platform Postgres, whatever its URI/credentials;
    # IDP_CATALOG_NAME overrides; a local SQLite catalog keeps a per-file name
    from idp.common import catalog
    monkeypatch.delenv("IDP_CATALOG_NAME", raising=False)
    monkeypatch.setenv("IDP_CATALOG_TYPE", "sql")
    for pw in ("pw", "rotated-pw"):
        monkeypatch.setenv("IDP_CATALOG_URI", f"postgresql+psycopg2://postgres:{pw}@db:5432/iceberg")
        assert catalog.catalog_name(load_config()) == "lake"
    monkeypatch.setenv("IDP_CATALOG_NAME", "lake_custom")
    assert catalog.catalog_name(load_config()) == "lake_custom"
    monkeypatch.delenv("IDP_CATALOG_NAME")
    monkeypatch.delenv("IDP_CATALOG_URI")
    local = catalog.catalog_name(load_config())
    assert local.startswith("lake_") and local != "lake"


def test_trino_defaults_and_env(monkeypatch):
    # SCN-FND-01.6 — the analytics tools' query engine comes from config/env, over
    # HTTPS, with no configured user (the identity comes from the MCP client)
    for var in ("IDP_TRINO_HOST", "IDP_TRINO_PORT", "IDP_TRINO_CATALOG", "IDP_TRINO_CA_CERT"):
        monkeypatch.delenv(var, raising=False)
    t = load_config().trino
    assert (t["host"], t["port"], t["catalog"], t["http_scheme"]) == ("127.0.0.1", 8085, "lake", "https")
    assert "user" not in t
    assert t["ca_cert"].endswith("deploy/platform/trino/.secrets/tls.crt") and t["ca_cert"].startswith("/")
    monkeypatch.setenv("IDP_TRINO_HOST", "trino.idp-platform.svc")
    monkeypatch.setenv("IDP_TRINO_PORT", "8443")
    monkeypatch.setenv("IDP_TRINO_CA_CERT", "/etc/ssl/trino.crt")
    t = load_config().trino
    assert (t["host"], t["port"], t["ca_cert"]) == ("trino.idp-platform.svc", 8443, "/etc/ssl/trino.crt")


def test_mcp_identity_via_env(monkeypatch):
    # REQ-FND-01: the MCP client credentials come from the environment; a client id enables it
    for var in ("IDP_MCP_TOKEN_URL", "IDP_MCP_CLIENT_ID", "IDP_MCP_CLIENT_SECRET"):
        monkeypatch.delenv(var, raising=False)
    a = load_config().mcp["auth"]
    assert a["enabled"] is False and a["token_url"].endswith("/realms/idp/protocol/openid-connect/token")
    monkeypatch.setenv("IDP_MCP_CLIENT_ID", "idp-mcp-tenant_b")
    monkeypatch.setenv("IDP_MCP_CLIENT_SECRET", "x")
    monkeypatch.setenv("IDP_MCP_TOKEN_URL", "http://kc:8080/realms/idp/protocol/openid-connect/token")
    a = load_config().mcp["auth"]
    assert (a["enabled"], a["client_id"], a["client_secret"]) == (True, "idp-mcp-tenant_b", "x")


def test_mlflow_registry_via_env(monkeypatch):
    # REQ-ML-04 — no tracking URI: the object-store fallback; the env selects MLflow
    monkeypatch.delenv("IDP_MLFLOW_TRACKING_URI", raising=False)
    cfg = load_config()
    assert cfg.mlflow["tracking_uri"] == "" and cfg.mlflow["refresh_seconds"] == 60
    assert cfg.credit["min_auc"] == 0.55 and cfg.fraud["min_pr_auc_lift"] == 1.5
    monkeypatch.setenv("IDP_MLFLOW_TRACKING_URI", "http://mlflow.idp-platform.svc:5000")
    assert load_config().mlflow["tracking_uri"] == "http://mlflow.idp-platform.svc:5000"


def test_auth_defaults_off_and_env(monkeypatch):
    # REQ-SRV-03 — auth off for local dev/tests; the cluster overlays enable it via env
    for var in ("IDP_AUTH_ENABLED", "IDP_AUTH_ISSUER", "IDP_AUTH_AUDIENCE"):
        monkeypatch.delenv(var, raising=False)
    a = load_config().auth
    assert a["enabled"] is False and a["tenant_claim"] == "tenant_id" and a["leeway_seconds"] == 30
    assert a["issuer"] == "http://keycloak.idp-platform.svc:8080/realms/idp"
    monkeypatch.setenv("IDP_AUTH_ENABLED", "true")
    monkeypatch.setenv("IDP_AUTH_ISSUER", "http://kc/realms/x")
    monkeypatch.setenv("IDP_AUTH_AUDIENCE", "aud-x")
    a = load_config().auth
    assert (a["enabled"], a["issuer"], a["audience"]) == (True, "http://kc/realms/x", "aud-x")
    monkeypatch.setenv("IDP_AUTH_ENABLED", "false")
    assert load_config().auth["enabled"] is False
