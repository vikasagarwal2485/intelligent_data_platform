"""Shared pytest fixtures."""
from __future__ import annotations

from pathlib import Path

import pytest

import pandas as pd

from idp.common import pii
from idp.common.config import PlatformConfig, StorageConfig, Tenant
from idp.synthetic.generator import generate, generate_for_tenants

SALTS = {"tenant_a": "salt-a", "tenant_b": "salt-b"}


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole test session (REQ-ORC-05).

    Stage functions call ``get_spark(cfg)``, which reuses this session, so the JVM
    starts once. Small shuffle-partition count keeps tiny test datasets fast.
    """
    from idp.common.config import load_config
    from idp.transforms.session import get_spark

    cfg = load_config()
    cfg.raw["spark"] = {"master": "local[2]", "driver_memory": "1g"}
    session = get_spark(cfg, app_name="idp-tests")
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()


def _tokenize_labels(labels: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for tenant_id, part in labels.groupby("tenant_id"):
        parts.append(pii.tokenize_columns(part, SALTS[str(tenant_id)], columns=("service_id",)))
    return pd.concat(parts, ignore_index=True)


@pytest.fixture(scope="session")
def small_dataset():
    """A small single-tenant dataset (fast)."""
    return generate("tenant_a", subscribers=80, days=30, fraud_rate=0.2, seed=7)


@pytest.fixture(scope="session")
def multi_tenant_dataset():
    """Two-tenant dataset for isolation tests."""
    return generate_for_tenants(
        ["tenant_a", "tenant_b"], subscribers=80, days=30, fraud_rate=0.2, seed=7
    )


@pytest.fixture(scope="session")
def bronze_frames(multi_tenant_dataset):
    ds = multi_tenant_dataset
    return {
        "parties": ds.parties, "accounts": ds.accounts, "agreements": ds.agreements,
        "products": ds.products, "voice_events": ds.voice_events,
        "data_events": ds.data_events, "recharge_events": ds.recharge_events,
        "fee_charges": ds.fee_charges,
    }


def lake_cfg(root) -> PlatformConfig:
    """A local-lake config for the test tenants (no S3), rooted at ``root``."""
    sc = StorageConfig(root=str(root), landing=str(root / "landing"), bronze=str(root / "bronze"),
                       silver=str(root / "silver"), gold=str(root / "gold"),
                       ml_artifacts=str(root / "ml"))
    tenants = [Tenant(t, t, "eu", s) for t, s in SALTS.items()]
    return PlatformConfig(raw={"s3": {"enabled": False}, "features": {"windows": [30, 90]}},
                          tenants=tenants, storage=sc)


def read_lake(cfg: PlatformConfig, layer: str, table: str) -> pd.DataFrame:
    """A lake table as pandas (tests assert in pandas; the stages never do)."""
    df = pd.read_parquet(Path(getattr(cfg.storage, layer)) / table)
    df["tenant_id"] = df["tenant_id"].astype(str)
    return df


@pytest.fixture(scope="session")
def lake(spark, tmp_path_factory, bronze_frames):
    """The multi-tenant dataset built once through the real stages:
    Bronze (landing) -> Silver -> Gold on Spark, into a session temp lake."""
    from idp.pipeline import to_bronze, to_gold, to_silver

    cfg = lake_cfg(tmp_path_factory.mktemp("lake"))
    to_bronze(cfg, bronze_frames)
    to_silver(cfg)
    to_gold(cfg)
    return cfg


@pytest.fixture(scope="session")
def silver(lake):
    """Silver tables (pandas views of the lake)."""
    return {t: read_lake(lake, "silver", t)
            for t in ("dim_subscriber", "fact_usage", "fact_recharge", "fact_fee_charge")}


@pytest.fixture(scope="session")
def credit_features(lake):
    return read_lake(lake, "gold", "credit_features")


@pytest.fixture(scope="session")
def fraud_features(lake):
    return read_lake(lake, "gold", "fraud_features")


@pytest.fixture(scope="session")
def labels(multi_tenant_dataset):
    return _tokenize_labels(multi_tenant_dataset.labels)


@pytest.fixture(scope="session")
def credit_labeled(credit_features, labels):
    return credit_features.merge(labels, on=["tenant_id", "service_id"], how="inner")


@pytest.fixture(scope="session")
def fraud_labeled(fraud_features, labels):
    return fraud_features.merge(labels, on=["tenant_id", "service_id"], how="inner")
