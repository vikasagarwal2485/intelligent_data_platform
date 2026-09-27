"""Configuration loading and env overrides (REQ-FND-01)."""
from __future__ import annotations

from idp.common.config import load_config


def test_config_exposes_all_sections():
    # SCN-FND-01.1
    cfg = load_config()
    for section in ("tenants", "storage", "kafka", "s3", "catalog", "lake", "timescale", "spark",
                    "synthetic", "features", "credit", "fraud"):
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