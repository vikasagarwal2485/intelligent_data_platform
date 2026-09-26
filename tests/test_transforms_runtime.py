"""Spark runtime helpers: session settings and lake locations (REQ-ORC-05, REQ-FND-04)."""
from __future__ import annotations

from pathlib import Path

from idp.common import storage
from idp.common.config import load_config
from idp.transforms import lake
from idp.transforms.session import RUNTIME_CONF, spark_conf


def test_master_defaults_to_local(monkeypatch):
    # SCN-ORC-05.2
    monkeypatch.delenv("IDP_SPARK_MASTER", raising=False)
    cfg = load_config()
    cfg.raw.pop("spark", None)
    assert spark_conf(cfg)["spark.master"] == "local[*]"


def test_master_from_config(monkeypatch):
    # SCN-ORC-05.2
    monkeypatch.setenv("IDP_SPARK_MASTER", "spark://master:7077")
    monkeypatch.setenv("IDP_SPARK_DRIVER_MEMORY", "3g")
    conf = spark_conf(load_config())
    assert conf["spark.master"] == "spark://master:7077"
    assert conf["spark.driver.memory"] == "3g"


def test_runtime_settings_always_present(monkeypatch):
    monkeypatch.delenv("IDP_S3_ENDPOINT", raising=False)
    conf = spark_conf(load_config())
    assert {k: conf[k] for k in RUNTIME_CONF} == RUNTIME_CONF
    assert not any(k.startswith("spark.hadoop.fs.s3a") for k in conf)


def test_s3a_settings_when_s3_enabled(monkeypatch):
    monkeypatch.setenv("IDP_S3_ENDPOINT", "http://minio:9000")
    conf = spark_conf(load_config())
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"


def test_spark_and_pandas_resolve_the_same_layer(monkeypatch):
    # SCN-FND-04.3
    monkeypatch.setenv("IDP_S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("IDP_S3_BUCKET_PREFIX", "idp")
    cfg = load_config()
    assert lake.layer_uri(cfg, "silver") == "s3a://idp-silver"
    assert storage.lake_uri(cfg, "silver") == "s3://idp-silver"
    monkeypatch.delenv("IDP_S3_ENDPOINT")
    cfg = load_config()
    assert Path(lake.layer_uri(cfg, "silver")) == Path(storage.lake_uri(cfg, "silver")).resolve()


def test_read_missing_table_is_none(spark, tmp_path):
    cfg = load_config()
    from idp.common.config import StorageConfig
    cfg.storage = StorageConfig(root=str(tmp_path), landing=str(tmp_path), bronze=str(tmp_path / "b"),
                                silver=str(tmp_path / "s"), gold=str(tmp_path / "g"),
                                ml_artifacts=str(tmp_path / "ml"))
    assert lake.read_table(spark, cfg, "silver", "nope") is None


def test_write_replaces_only_present_tenants(spark, tmp_path, monkeypatch):
    monkeypatch.delenv("IDP_S3_ENDPOINT", raising=False)
    from idp.common.config import StorageConfig
    cfg = load_config()
    cfg.storage = StorageConfig(root=str(tmp_path), landing=str(tmp_path), bronze=str(tmp_path / "b"),
                                silver=str(tmp_path / "s"), gold=str(tmp_path / "g"),
                                ml_artifacts=str(tmp_path / "ml"))
    lake.write_table(spark.createDataFrame([("tenant_a", 1), ("tenant_b", 2)], ["tenant_id", "x"]),
                     cfg, "silver", "t")
    lake.write_table(spark.createDataFrame([("tenant_a", 9)], ["tenant_id", "x"]), cfg, "silver", "t")
    rows = {r["tenant_id"]: r["x"] for r in lake.read_table(spark, cfg, "silver", "t").collect()}
    assert rows == {"tenant_a": 9, "tenant_b": 2}


def test_spark_tokens_equal_python_tokens(spark):
    # SCN-FND-03.3 — Spark Silver tokens must join labels tokenized in Python
    from pyspark.sql import functions as F

    from idp.common import pii
    from idp.transforms.tokens import salt_col, strip_uuid, token
    salts = {"tenant_a": "salt-a", "tenant_b": "salt-b"}
    rows = [("tenant_a", "46700000001"), ("tenant_b", "46700000001"), ("tenant_a", None),
            ("tenant_c", "46700000002")]                 # tenant_c -> default salt
    df = spark.createDataFrame(rows, "tenant_id string, msisdn string")
    got = [r["tok"] for r in df.select(token(F.col("msisdn"), salt_col(salts)).alias("tok")).collect()]
    expected = [pii.tokenize_value(v, salts.get(t, f"default-salt-{t}")) for t, v in rows]
    assert got == expected
    assert got[0] != got[1] and got[2] is None


def test_strip_uuid_matches_python(spark):
    from pyspark.sql import functions as F

    from idp.curation.jsonapi import strip_uuid_wrapper
    from idp.transforms.tokens import strip_uuid
    vals = ["UUID(116100030984920447)", "plain-id", "UUID()"]
    df = spark.createDataFrame([(v,) for v in vals], "v string")
    got = [r[0] for r in df.select(strip_uuid(F.col("v"))).collect()]
    assert got == [strip_uuid_wrapper(v) for v in vals]
