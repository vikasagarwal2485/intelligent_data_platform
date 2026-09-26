"""Configuration loading and env overrides (REQ-FND-01)."""
from __future__ import annotations

from idp.common.config import load_config


def test_config_exposes_all_sections():
    # SCN-FND-01.1
    cfg = load_config()
    for section in ("tenants", "storage", "kafka", "s3", "timescale", "spark",
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
