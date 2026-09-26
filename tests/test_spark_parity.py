"""pandas <-> Spark engine parity (REQ-ORC-04).

Runs the pandas stages and the PySpark jobs (`pipelines/spark/*_job.py`) on the
same input and asserts identical tables: same columns in the same order, the same
logical types, equal integer values and real values within 1e-6. Marked `spark`
and skipped unless pyspark (and a JDK) is available: `pip install -e .[spark]`.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")

from idp.common.config import PlatformConfig, StorageConfig, Tenant  # noqa: E402
from idp.features.credit_features import CREDIT_FEATURES  # noqa: E402
from idp.features.fraud_features import FRAUD_FEATURES  # noqa: E402
from idp.pipeline import ENTITIES, to_bronze, to_gold, to_scores, to_silver, train  # noqa: E402
from idp.synthetic.generator import generate_for_tenants  # noqa: E402

pytestmark = pytest.mark.spark

REPO = Path(__file__).resolve().parents[1]
SALTS = {"tenant_a": "salt-a", "tenant_b": "salt-b"}
SILVER_KEYS = {
    "dim_subscriber": ["tenant_id", "service_id"],
    "fact_usage": ["tenant_id", "id"],
    "fact_recharge": ["tenant_id", "id"],
    "fact_fee_charge": ["tenant_id", "id"],
}
GOLD_KEY = ["tenant_id", "service_id"]


def _load_job(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / "pipelines" / "spark" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


silver_job = _load_job("silver_job")
gold_job = _load_job("gold_job")


def _cfg(root: Path) -> PlatformConfig:
    sc = StorageConfig(root=str(root), landing=str(root / "landing"), bronze=str(root / "bronze"),
                       silver=str(root / "silver"), gold=str(root / "gold"),
                       ml_artifacts=str(root / "ml"))
    tenants = [Tenant(t, t, "eu", s) for t, s in SALTS.items()]
    return PlatformConfig(raw={"s3": {"enabled": False}, "features": {"windows": [7, 30, 90]}},
                          tenants=tenants, storage=sc)


def _read(uri) -> pd.DataFrame:
    df = pd.read_parquet(uri)
    df["tenant_id"] = df["tenant_id"].astype(str)
    return df


def _family(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):
        return "bool"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "datetime"
    if pd.api.types.is_numeric_dtype(s):
        return "number"
    return "other"


def assert_same_table(left: pd.DataFrame, right: pd.DataFrame, key: list[str], atol: float = 1e-6):
    """Same columns (and order), same logical types, same rows, values within atol."""
    assert list(left.columns) == list(right.columns)
    assert len(left) == len(right)
    left = left.sort_values(key).reset_index(drop=True)
    right = right.sort_values(key).reset_index(drop=True)
    for col in left.columns:
        a, b = left[col], right[col]
        # an all-null column carries no type information (e.g. data events' imei)
        if not (a.isna().all() or b.isna().all()):
            assert _family(a) == _family(b), f"{col}: {a.dtype} vs {b.dtype}"
        if _family(a) == "number" and _family(b) == "number":
            np.testing.assert_allclose(a.astype(float), b.astype(float), atol=atol, rtol=0,
                                       err_msg=col)
        elif _family(a) == "datetime":
            assert (pd.to_datetime(a, utc=True).fillna(pd.Timestamp(0, tz="UTC"))
                    == pd.to_datetime(b, utc=True).fillna(pd.Timestamp(0, tz="UTC"))).all(), col
        else:
            na, nb = a.astype(object).where(a.notna(), None), b.astype(object).where(b.notna(), None)
            assert na.tolist() == nb.tolist(), col


# --------------------------------------------------------------------------- #
# fixtures: one Bronze, both engines                                          #
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]").appName("idp-parity")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    yield session
    session.stop()


@pytest.fixture(scope="module")
def dataset():
    return generate_for_tenants(list(SALTS), subscribers=40, days=21, fraud_rate=0.2,
                                default_rate=0.2, seed=11)


@pytest.fixture(scope="module")
def lakes(tmp_path_factory, dataset, spark):
    """pandas lake and spark lake sharing the same Bronze; Silver built by each engine."""
    pd_cfg = _cfg(tmp_path_factory.mktemp("pandas"))
    sp_root = tmp_path_factory.mktemp("spark")
    to_bronze(pd_cfg, {e: getattr(dataset, e) for e in ENTITIES})
    to_silver(pd_cfg)                                   # reads Bronze from disk, like the job
    silver_job.build_silver(spark, salts=SALTS, bronze=pd_cfg.storage.bronze,
                            silver=str(sp_root / "silver"))
    return pd_cfg, sp_root


# --------------------------------------------------------------------------- #
# Silver parity (SCN-ORC-04.1)                                                #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("table", list(SILVER_KEYS))
def test_silver_parity(lakes, table):
    pd_cfg, sp_root = lakes
    assert_same_table(_read(Path(pd_cfg.storage.silver) / table),
                      _read(sp_root / "silver" / table), SILVER_KEYS[table])


# --------------------------------------------------------------------------- #
# Gold parity (SCN-ORC-04.2) — both engines read the SAME (pandas) Silver      #
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def gold_lakes(lakes, spark):
    pd_cfg, sp_root = lakes
    to_gold(pd_cfg)                                    # reads pandas Silver from disk
    gold_job.build_gold(spark, silver=pd_cfg.storage.silver, gold=str(sp_root / "gold"),
                        windows=pd_cfg.features["windows"])
    return pd_cfg, sp_root


@pytest.mark.parametrize("table,features", [("credit_features", CREDIT_FEATURES),
                                            ("fraud_features", FRAUD_FEATURES)])
def test_gold_parity(gold_lakes, table, features):
    pd_cfg, sp_root = gold_lakes
    pdf = _read(Path(pd_cfg.storage.gold) / table)
    sdf = _read(sp_root / "gold" / table)
    assert list(sdf.columns) == ["service_id", *features, "tenant_id"]
    assert not sdf.duplicated(GOLD_KEY).any()
    assert_same_table(pdf, sdf, GOLD_KEY)


def test_spark_feature_lists_match_pandas():
    # the job cannot import idp, so it carries copies — this guards against drift
    assert gold_job.CREDIT_FEATURES == CREDIT_FEATURES
    assert gold_job.FRAUD_FEATURES == FRAUD_FEATURES


# --------------------------------------------------------------------------- #
# Spark-built Gold is scoreable (SCN-ORC-04.3)                                 #
# --------------------------------------------------------------------------- #
def test_spark_gold_is_scoreable(gold_lakes, dataset):
    pd_cfg, sp_root = gold_lakes
    silver = to_silver(pd_cfg, persist=False)
    models = train(pd_cfg, dataset=dataset, silver=silver, persist=False)["models"]
    sp_cfg = _cfg(sp_root)                             # Gold on disk = the Spark output
    out = to_scores(sp_cfg, models=models, persist=False)
    assert out["score_tables"] == ["credit_scores", "fraud_scores"]
    n = len(_read(sp_root / "gold" / "credit_features"))
    assert len(out["credit_scores"]) == n and len(out["fraud_scores"]) == n


# --------------------------------------------------------------------------- #
# Spark replaces per tenant (SCN-ORC-04.4)                                     #
# --------------------------------------------------------------------------- #
def test_spark_overwrite_is_per_tenant(spark, tmp_path):
    silver_job.configure(spark)
    uri = str(tmp_path / "t")
    spark.createDataFrame([("tenant_a", 1), ("tenant_b", 2)], ["tenant_id", "x"]) \
        .write.mode("overwrite").partitionBy("tenant_id").parquet(uri)
    silver_job._write(spark.createDataFrame([("tenant_a", 9)], ["tenant_id", "x"]), uri)
    got = _read(uri)
    assert dict(zip(got["tenant_id"], got["x"])) == {"tenant_a": 9, "tenant_b": 2}
