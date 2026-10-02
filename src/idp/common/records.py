"""Canonical records as plain JSON-ready dicts (REQ-STR-01, REQ-BRZ-05).

pandas marks a missing value as NaN in its string and float columns, and
``to_dict()`` hands that NaN through, which is not valid JSON and which a TM Forum
resource can't carry (a null field is simply absent there). Every path that turns
canonical frames into records (in-memory production, the TMF landing writer, and so
the adapter's output) uses :func:`to_records`, so a missing value is ``None``
everywhere and the paths produce the same messages.
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd


def _clean(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """The frame's rows as dicts in column order, with missing values as ``None``."""
    return [{k: _clean(v) for k, v in row.items()} for row in df.to_dict("records")]
