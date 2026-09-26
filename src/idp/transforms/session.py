"""SparkSession for the Silver/Gold stages (REQ-ORC-05).

The execution environment only chooses the Spark master and resources
(``cfg.spark``); the transforms are the same everywhere.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from idp.common.config import PlatformConfig

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"

# Session behaviour the transforms rely on (and the golden outputs were produced
# with): UTC day arithmetic, per-tenant (dynamic) partition overwrite, and Parquet
# timestamps that pandas/ML readers load as tz-aware UTC.
RUNTIME_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
}


def _hadoop_aws_bundled() -> bool:
    """True when the S3A jars ship with pyspark (as in the idp image)."""
    import pyspark
    return any((Path(pyspark.__file__).parent / "jars").glob("hadoop-aws-*.jar"))


def spark_conf(cfg: PlatformConfig) -> dict[str, str]:
    """Builder settings for ``cfg``: master, driver memory, and S3A when enabled."""
    conf: dict[str, str] = {
        "spark.master": cfg.spark["master"],
        "spark.driver.memory": str(cfg.spark["driver_memory"]),
        "spark.ui.showConsoleProgress": "false",
        **RUNTIME_CONF,
    }
    s3 = cfg.s3
    if s3.get("enabled"):
        endpoint = str(s3["endpoint"])
        conf.update({
            "spark.hadoop.fs.s3a.endpoint": endpoint,
            "spark.hadoop.fs.s3a.access.key": str(s3["access_key"]),
            "spark.hadoop.fs.s3a.secret.key": str(s3["secret_key"]),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        })
    return conf


def get_spark(cfg: PlatformConfig, app_name: str = "idp") -> Any:
    """Build (or reuse) the process's SparkSession for ``cfg``.

    Static settings (master, driver memory, jars) take effect when the first
    session in the process starts; the runtime SQL and S3A settings are applied
    on every call so a reused session always matches ``cfg``.
    """
    from pyspark.sql import SparkSession

    conf = spark_conf(cfg)
    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    if cfg.s3.get("enabled") and not _hadoop_aws_bundled():
        builder = builder.config("spark.jars.packages", HADOOP_AWS_PACKAGE)
    spark = builder.getOrCreate()

    for key, value in RUNTIME_CONF.items():
        spark.conf.set(key, value)
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in conf.items():
        if key.startswith("spark.hadoop."):
            hadoop.set(key[len("spark.hadoop."):], value)
    return spark
