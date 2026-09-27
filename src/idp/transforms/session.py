"""SparkSession for the Silver/Gold stages (REQ-ORC-05).

The execution environment only chooses the Spark master and resources
(``cfg.spark``); the transforms are the same everywhere.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from idp.common.config import PlatformConfig

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
# Iceberg lake tables (REQ-FND-04). Versions pinned by the task-1.1 spike of
# iceberg-incremental-lakehouse; the idp image bundles the same jars.
ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0",
    "org.xerial:sqlite-jdbc:3.46.1.3",       # local SQL catalog
    "org.postgresql:postgresql:42.7.4",      # cluster SQL catalog (platform Postgres)
)
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

# Session behaviour the transforms rely on (and the golden outputs were produced
# with): UTC day arithmetic, per-tenant (dynamic) partition overwrite, and Parquet
# timestamps that pandas/ML readers load as tz-aware UTC.
RUNTIME_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
}


def _bundled(pattern: str) -> bool:
    """True when a jar ships in pyspark's jars dir (as in the idp image)."""
    import pyspark
    return any((Path(pyspark.__file__).parent / "jars").glob(pattern))


def _hadoop_aws_bundled() -> bool:
    return _bundled("hadoop-aws-*.jar")


def _packages(cfg: PlatformConfig) -> list[str]:
    """Maven packages to fetch when not bundled (local dev/CI; cached in ~/.ivy2)."""
    pkgs = [] if _bundled("iceberg-spark-runtime-*.jar") else list(ICEBERG_PACKAGES)
    if cfg.s3.get("enabled") and not _hadoop_aws_bundled():
        pkgs.append(HADOOP_AWS_PACKAGE)
    return pkgs


def spark_conf(cfg: PlatformConfig) -> dict[str, str]:
    """Builder settings for ``cfg``: master, driver memory, and S3A when enabled."""
    conf: dict[str, str] = {
        "spark.master": cfg.spark["master"],
        "spark.driver.memory": str(cfg.spark["driver_memory"]),
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
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
            # Iceberg table locations are s3://; the Hadoop-FS side of the Iceberg
            # procedures (remove_orphan_files lists a table's files) needs a
            # filesystem for that scheme too
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
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
    packages = _packages(cfg)
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    spark = builder.getOrCreate()

    for key, value in RUNTIME_CONF.items():
        spark.conf.set(key, value)
    from idp.transforms.lake import attach
    attach(spark, cfg)                       # the lake's Iceberg catalog (REQ-FND-04)
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in conf.items():
        if key.startswith("spark.hadoop."):
            hadoop.set(key[len("spark.hadoop."):], value)
    return spark
