"""Tests for multi-tenancy / data isolation (plan 1.5)."""
from __future__ import annotations

import pandas as pd
import pytest

from idp.common.tenancy import (
    TenantIsolationError,
    assert_single_tenant,
    require_tenant_column,
    scope_to_tenant,
    stamp_tenant,
)


def test_require_tenant_column_raises_when_missing():
    with pytest.raises(TenantIsolationError):
        require_tenant_column(pd.DataFrame({"x": [1]}))


def test_scope_to_tenant_returns_only_that_tenant():
    df = pd.DataFrame({"tenant_id": ["a", "a", "b"], "v": [1, 2, 3]})
    scoped = scope_to_tenant(df, "a")
    assert set(scoped["tenant_id"]) == {"a"}
    assert len(scoped) == 2


def test_scope_to_tenant_blocks_cross_tenant_read():
    """A tenant_a caller must never see tenant_b rows."""
    df = pd.DataFrame({"tenant_id": ["a", "b"], "secret": ["a-data", "b-data"]})
    scoped = scope_to_tenant(df, "a")
    assert "b-data" not in scoped["secret"].tolist()


def test_assert_single_tenant_detects_mix():
    df = pd.DataFrame({"tenant_id": ["a", "b"]})
    with pytest.raises(TenantIsolationError):
        assert_single_tenant(df)


def test_stamp_tenant_is_idempotent():
    df = pd.DataFrame({"v": [1, 2]})
    out = stamp_tenant(df, "a")
    assert (out["tenant_id"] == "a").all()
