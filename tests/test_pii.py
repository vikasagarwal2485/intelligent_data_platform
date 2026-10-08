"""Tests for GDPR PII handling."""
from __future__ import annotations

import pandas as pd

from idp.common import pii


def test_tokenize_is_deterministic_and_salted():
    assert pii.tokenize_value("595984920447", "salt-a") == pii.tokenize_value("595984920447", "salt-a")
    assert pii.tokenize_value("595984920447", "salt-a") != pii.tokenize_value("595984920447", "salt-b")


def test_tokenize_masks_raw_value():
    tok = pii.tokenize_value("123456789", "s")
    assert "123456789" not in tok
    assert tok.startswith("tok_")


def test_tokenize_handles_none():
    assert pii.tokenize_value(None, "s") is None


def test_tokenize_columns_only_touches_pii():
    df = pd.DataFrame({"msisdn": ["111"], "account_type": ["salaried"]})
    out = pii.tokenize_columns(df, "s")
    assert out["msisdn"].iloc[0].startswith("tok_")
    assert out["account_type"].iloc[0] == "salaried"


def test_filter_by_consent():
    df = pd.DataFrame({"consent_analytics": [True, False], "v": [1, 2]})
    out = pii.filter_by_consent(df, "analytics")
    assert out["v"].tolist() == [1]


def test_filter_by_consent_noop_when_absent():
    df = pd.DataFrame({"v": [1, 2]})
    assert len(pii.filter_by_consent(df, "analytics")) == 2
