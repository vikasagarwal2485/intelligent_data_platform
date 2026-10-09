"""Shared pytest fixtures."""
from __future__ import annotations

from pathlib import Path

import os

import pytest

import pandas as pd

# Every Spark session in the tests is local[2], whichever test creates it first: a
# stage's shuffle parallelism and conform parallelism follow the session's cores
# (REQ-ORC-07), so a local[*] session would run tiny test data on dozens of
# partitions and Python workers. Tests that need another master set it themselves.
os.environ.setdefault("IDP_SPARK_MASTER", "local[2]")

from idp.common import pii
from idp.common.config import PlatformConfig, StorageConfig, Tenant
from idp.synthetic.generator import generate, generate_for_tenants
from trino_stack import trino_lake  # noqa: F401 - the compose stack's lake (marker `trino`)

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
    """The dataset as raw Bronze rows per table: the TMF events the producer would
    publish, as the consumer lands them (REQ-BRZ-01)."""
    from idp.ingestion.raw import raw_events
    return raw_events(multi_tenant_dataset)


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
    from idp.common import catalog
    return catalog.read_table(cfg, layer, table)


def rewrite_table(cfg: PlatformConfig, layer: str, table: str, df: pd.DataFrame,
                  drop_columns: tuple[str, ...] = ()) -> None:
    """Replace a lake table's contents with ``df`` (tests simulating upstream changes);
    ``drop_columns`` are first removed from the table schema."""
    from idp.common import catalog
    if drop_columns:
        t = catalog.load_catalog(cfg).load_table(f"{layer}.{table}")
        with t.update_schema() as upd:
            for col in drop_columns:
                upd.delete_column(col)
        df = df.drop(columns=[c for c in drop_columns if c in df.columns])
    catalog.replace_table(cfg, layer, table, df)


def clone_lake(src: PlatformConfig, root: Path, *, layers=("silver",), drop_tenant: str | None = None,
               tables: dict | None = None) -> PlatformConfig:
    """A new local lake (own catalog) holding copies of ``src``'s tables in ``layers``.

    ``drop_tenant`` names a table copied WITHOUT its tenant_id column (to test
    isolation errors). ``tables`` optionally restricts the copied table names.
    """
    from idp.common import catalog
    dst = lake_cfg(root)
    cat = catalog.load_catalog(src)
    for layer in layers:
        for _, name in cat.list_tables(layer):
            if tables is not None and name not in tables:
                continue
            df = catalog.read_table(src, layer, name)
            if name == drop_tenant:
                df = df.drop(columns=["tenant_id"])
            catalog.append_table(dst, layer, name, df)
    return dst


@pytest.fixture(scope="session")
def lake(spark, tmp_path_factory, bronze_frames):
    """The multi-tenant dataset built once through the real stages:
    raw Bronze -> Silver (conform + curate) -> Gold on Spark, into a session temp lake."""
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
            for t in ("dim_subscriber", "dim_customer", "dim_billing_account", "dim_product",
                      "fact_usage", "fact_recharge", "fact_fee_charge")}


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


@pytest.fixture(scope="session")
def model_artifacts(credit_labeled, fraud_labeled, tmp_path_factory):
    """A local model store with credit and fraud models fitted on the session lake's
    labelled Gold features: what the API needs, without running a pipeline."""
    from idp.common import storage
    from idp.ml.credit_model import CreditLimitModel
    from idp.ml.fraud_model import FraudModel

    d = tmp_path_factory.mktemp("ml")
    cfg = type("C", (), {"s3": {}})()
    storage.save_model(CreditLimitModel().fit(credit_labeled, credit_labeled["will_default"]),
                       str(d / "credit_model.joblib"), cfg)
    storage.save_model(FraudModel().fit(fraud_labeled, fraud_labeled["is_fraud"]),
                       str(d / "fraud_model.joblib"), cfg)
    return d
