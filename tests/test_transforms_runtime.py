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


def test_stage_profile_applied_off_local(monkeypatch):
    # SCN-ORC-07.1 at conf level: executors from the stage profile, driver its own
    monkeypatch.setenv("IDP_SPARK_MASTER", "k8s://https://kubernetes.default.svc")
    monkeypatch.delenv("IDP_SPARK_DRIVER_MEMORY", raising=False)
    cfg = load_config()
    cfg.raw["spark"]["profiles"]["gold"] = {
        "driver": {"cores": 1, "memory": "1g", "memoryOverhead": "512m"},
        "executor": {"instances": 3, "cores": 1, "memory": "2g", "memoryOverhead": "512m"}}
    conf = spark_conf(cfg, stage="gold")
    assert conf["spark.executor.instances"] == "3"
    assert conf["spark.executor.memory"] == "2g" and conf["spark.executor.memoryOverhead"] == "512m"
    assert conf["spark.driver.memory"] == "1g" and conf["spark.driver.memoryOverhead"] == "512m"


def test_local_master_ignores_executor_settings(monkeypatch):
    # SCN-ORC-05.2 — a local session has no executors; only the driver heap applies
    monkeypatch.delenv("IDP_SPARK_MASTER", raising=False)
    monkeypatch.delenv("IDP_SPARK_DRIVER_MEMORY", raising=False)
    cfg = load_config()
    conf = spark_conf(cfg, stage="silver")
    assert conf["spark.master"] == "local[*]"
    assert conf["spark.driver.memory"] == cfg.spark_profile("silver")["driver"]["memory"]
    assert not any(k.startswith(("spark.executor.", "spark.driver.memoryOverhead")) for k in conf)


def test_driver_memory_env_beats_profile(monkeypatch):
    monkeypatch.setenv("IDP_SPARK_DRIVER_MEMORY", "3g")
    assert spark_conf(load_config(), stage="train")["spark.driver.memory"] == "3g"
    monkeypatch.delenv("IDP_SPARK_DRIVER_MEMORY")
    assert spark_conf(load_config(), stage="train")["spark.driver.memory"] == "2g"   # the train profile
    assert spark_conf(load_config(), stage="nope")["spark.driver.memory"] == "1g"    # unknown -> spark.driver_memory


def test_spark_submit_owns_master_and_resources(monkeypatch):
    # In the operator's driver pod the manifest's resources must win over the image's
    # profile, so scaling a stage only needs a re-rendered manifest (REQ-ORC-07)
    from idp.transforms.session import builder_conf
    monkeypatch.setenv("IDP_SPARK_MASTER", "k8s://https://kubernetes.default.svc:443")
    monkeypatch.delenv("PYSPARK_GATEWAY_PORT", raising=False)
    cfg = load_config()
    assert "spark.executor.instances" in builder_conf(cfg, "silver")          # not launched by spark-submit
    monkeypatch.setenv("PYSPARK_GATEWAY_PORT", "12345")                       # launched by spark-submit
    conf = builder_conf(cfg, "silver")
    assert not any(k == "spark.master" or k.startswith(("spark.driver.", "spark.executor.")) for k in conf)
    assert conf["spark.sql.extensions"] and conf["spark.sql.session.timeZone"] == "UTC"   # the rest still applies


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
    assert conf["spark.hadoop.fs.s3.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"   # s3:// table locations


def test_spark_and_pandas_resolve_the_same_layer(monkeypatch):
    # layer roots (used for non-table data such as stream checkpoints)
    monkeypatch.setenv("IDP_S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("IDP_S3_BUCKET_PREFIX", "idp")
    cfg = load_config()
    assert lake.layer_uri(cfg, "silver") == "s3a://idp-silver"
    assert storage.lake_uri(cfg, "silver") == "s3://idp-silver"
    monkeypatch.delenv("IDP_S3_ENDPOINT")
    cfg = load_config()
    assert Path(lake.layer_uri(cfg, "silver")) == Path(storage.lake_uri(cfg, "silver")).resolve()


def test_read_missing_table_is_none(spark, tmp_path):
    from conftest import lake_cfg
    assert lake.read_table(spark, lake_cfg(tmp_path), "silver", "nope") is None


def test_overwrite_replaces_only_present_tenants(spark, tmp_path):
    from conftest import lake_cfg
    cfg = lake_cfg(tmp_path)
    lake.overwrite_tenants(spark.createDataFrame([("tenant_a", 1), ("tenant_b", 2)], ["tenant_id", "x"]),
                           cfg, "silver", "t")
    lake.overwrite_tenants(spark.createDataFrame([("tenant_a", 9)], ["tenant_id", "x"]), cfg, "silver", "t")
    rows = {r["tenant_id"]: r["x"] for r in lake.read_table(spark, cfg, "silver", "t").collect()}
    assert rows == {"tenant_a": 9, "tenant_b": 2}


def test_spark_and_python_see_each_others_commits(spark, tmp_path):
    # SCN-FND-04.3 — one catalog: Spark and pyiceberg read each other's commits
    import pandas as pd

    from conftest import lake_cfg
    from idp.common import catalog
    cfg = lake_cfg(tmp_path)
    catalog.append_table(cfg, "bronze", "t", pd.DataFrame({"tenant_id": ["tenant_a"], "id": ["1"]}))
    assert lake.read_table(spark, cfg, "bronze", "t").count() == 1          # Python -> Spark
    lake.merge_insert_new(spark.createDataFrame([("tenant_a", "1"), ("tenant_b", "2")], ["tenant_id", "id"]),
                          cfg, "bronze", "t", keys=["tenant_id", "id"])
    assert sorted(catalog.read_table(cfg, "bronze", "t")["id"]) == ["1", "2"]  # Spark -> Python (no dup)
    catalog.append_table(cfg, "bronze", "t", pd.DataFrame({"tenant_id": ["tenant_a"], "id": ["3"]}))
    assert lake.read_table(spark, cfg, "bronze", "t").count() == 3          # same session sees it


def test_ensure_table_adds_new_columns(spark, tmp_path):
    from conftest import lake_cfg
    cfg = lake_cfg(tmp_path)
    lake.overwrite_tenants(spark.createDataFrame([("tenant_a", 1)], ["tenant_id", "x"]), cfg, "silver", "t")
    lake.merge_insert_new(spark.createDataFrame([("tenant_a", 2, "new")], ["tenant_id", "x", "extra"]),
                          cfg, "silver", "t", keys=["tenant_id", "x"])
    got = {r["x"]: r["extra"] for r in lake.read_table(spark, cfg, "silver", "t").collect()}
    assert got == {1: None, 2: "new"}


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

    from idp.ingestion.jsonapi import strip_uuid_wrapper
    from idp.transforms.tokens import strip_uuid
    vals = ["UUID(116100030984920447)", "plain-id", "UUID()"]
    df = spark.createDataFrame([(v,) for v in vals], "v string")
    got = [r[0] for r in df.select(strip_uuid(F.col("v"))).collect()]
    assert got == [strip_uuid_wrapper(v) for v in vals]
