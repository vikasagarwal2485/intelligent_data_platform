"""Golden-file regression tests for the Silver and Gold stages (REQ-ORC-05).

The stages run on a fixed synthetic Bronze (tests/golden_data.py) and must
reproduce the reference tables in tests/golden/ (captured by
tests/golden/make_golden.py): same columns in the same order, same logical types,
integer values exact and real values within 1e-6.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from golden_data import GOLDEN_GOLD, GOLDEN_KEYS, GOLDEN_SILVER, golden_cfg, land_golden_bronze
from idp.common import catalog
from idp.pipeline import to_gold, to_silver

GOLDEN = Path(__file__).resolve().parent / "golden"


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


def _tenant_first(df: pd.DataFrame) -> pd.DataFrame:
    """The golden files were written as Hive-partitioned Parquet, which moves the
    partition column last; Iceberg keeps schema order. Compare with tenant_id first."""
    return df[["tenant_id", *[c for c in df.columns if c != "tenant_id"]]]


def assert_same_table(expected: pd.DataFrame, actual: pd.DataFrame, key: list[str],
                      atol: float = 1e-6) -> None:
    """Same columns (and order), same logical types, same rows, values within atol."""
    expected, actual = _tenant_first(expected), _tenant_first(actual)
    assert list(actual.columns) == list(expected.columns)
    assert len(actual) == len(expected)
    expected = expected.sort_values(key).reset_index(drop=True)
    actual = actual.sort_values(key).reset_index(drop=True)
    for col in expected.columns:
        a, b = expected[col], actual[col]
        # an all-null column carries no type information (e.g. data events' imei)
        if not (a.isna().all() or b.isna().all()):
            assert _family(a) == _family(b), f"{col}: {a.dtype} vs {b.dtype}"
        if _family(a) == "number" and _family(b) == "number":
            np.testing.assert_allclose(a.astype(float), b.astype(float), atol=atol, rtol=0,
                                       err_msg=col)
        elif _family(a) == "datetime":
            zero = pd.Timestamp(0, tz="UTC")
            assert (pd.to_datetime(a, utc=True).fillna(zero)
                    == pd.to_datetime(b, utc=True).fillna(zero)).all(), col
        else:
            na = a.astype(object).where(a.notna(), None)
            nb = b.astype(object).where(b.notna(), None)
            assert na.tolist() == nb.tolist(), col


@pytest.fixture(scope="module")
def golden_lake(tmp_path_factory):
    cfg = golden_cfg(tmp_path_factory.mktemp("golden"))
    land_golden_bronze(cfg)
    to_silver(cfg)
    to_gold(cfg)
    return cfg


@pytest.mark.parametrize("table", GOLDEN_SILVER)
def test_silver_matches_golden(golden_lake, table):
    assert_same_table(_read(GOLDEN / "silver" / table),
                      catalog.read_table(golden_lake, "silver", table), GOLDEN_KEYS[table])


@pytest.mark.parametrize("table", GOLDEN_GOLD)
def test_gold_matches_golden(golden_lake, table):
    assert_same_table(_read(GOLDEN / "gold" / table),
                      catalog.read_table(golden_lake, "gold", table), GOLDEN_KEYS[table])
