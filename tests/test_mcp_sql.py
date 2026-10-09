"""The MCP query layer over Trino, with a fake DB-API connection (REQ-MCP-06)."""
from __future__ import annotations

import pandas as pd
import pytest
import trino.exceptions as te

from idp.common.config import load_config
from idp.mcp import sql


class _Cur:
    def __init__(self, conn):
        self.conn = conn
        self.description = None

    def execute(self, q, params):
        self.conn.calls.append((q, params))
        if self.conn.error:
            raise self.conn.error
        self.description = [(c,) for c in self.conn.cols]

    def fetchall(self):
        return self.conn.rows


class _Conn:
    def __init__(self, rows=(), cols=(), error=None):
        self.rows, self.cols, self.error, self.calls, self.closed = list(rows), list(cols), error, [], False

    def cursor(self):
        return _Cur(self)

    def close(self):
        self.closed = True


def _fake(monkeypatch, **kw):
    conn = _Conn(**kw)
    monkeypatch.setattr(sql, "connect", lambda cfg: conn)
    return conn


def _user_error(name):
    return te.TrinoUserError({"errorName": name, "message": f"{name} happened"})


def test_query_binds_parameters_and_returns_a_frame(monkeypatch):
    conn = _fake(monkeypatch, rows=[("tenant_a", 3)], cols=["tenant_id", "n"])
    df = sql.query(load_config(), "SELECT tenant_id, count(*) n FROM silver.x WHERE tenant_id = ?",
                   ["tenant_a"])
    assert df.to_dict("records") == [{"tenant_id": "tenant_a", "n": 3}]
    assert conn.calls == [("SELECT tenant_id, count(*) n FROM silver.x WHERE tenant_id = ?", ["tenant_a"])]
    assert conn.closed


def test_table_not_written_is_empty(monkeypatch):
    for name in ("TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND"):
        _fake(monkeypatch, error=_user_error(name))
        assert sql.query(load_config(), "SELECT 1").empty


def test_access_denied_and_unreachable_are_lake_read_errors(monkeypatch):
    cfg = load_config()
    _fake(monkeypatch, error=_user_error("PERMISSION_DENIED"))
    with pytest.raises(sql.LakeReadError) as e:
        sql.query(cfg, "INSERT INTO silver.x VALUES (1)")
    assert e.value.source == sql.endpoint(cfg) and "access denied" in str(e.value)

    def down(cfg):
        raise te.TrinoConnectionError("connection refused")
    monkeypatch.setattr(sql, "connect", down)
    with pytest.raises(sql.LakeReadError) as e:
        sql.query(cfg, "SELECT 1")
    assert e.value.source == "https://127.0.0.1:8085"


def test_a_bad_query_is_a_bug_not_empty(monkeypatch):
    _fake(monkeypatch, error=_user_error("SYNTAX_ERROR"))
    with pytest.raises(te.TrinoUserError):
        sql.query(load_config(), "SELEC 1")


def test_available_tenants_from_partition_metadata(monkeypatch):
    conn = _fake(monkeypatch, rows=[("tenant_b",), ("tenant_a",)], cols=["tenant_id"])
    assert sql.available_tenants(load_config()) == ["tenant_a", "tenant_b"]
    assert '"dim_subscriber$partitions"' in conn.calls[0][0]
    _fake(monkeypatch, error=_user_error("TABLE_NOT_FOUND"))
    assert sql.available_tenants(load_config()) == []


class _Provider:
    """A token provider that hands out a new token each call (as renewal would)."""
    source = "http://kc/token"
    tenant = "tenant_a"

    def __init__(self):
        self.n = 0

    def token(self):
        self.n += 1
        return f"token-{self.n}"


def test_connect_authenticates_with_a_current_token_over_verified_https(monkeypatch):
    # REQ-MCP-06: every connection carries the identity's current token; HTTPS is
    # verified with the configured CA certificate
    import trino
    from idp.mcp import auth

    seen = []
    monkeypatch.setattr(trino.dbapi, "connect", lambda **kw: seen.append(kw) or _Conn())
    monkeypatch.setenv("IDP_TRINO_CA_CERT", "/tmp/ca.crt")
    p = _Provider()
    auth.use_provider(p)
    try:
        cfg = load_config()
        sql.connect(cfg)
        sql.connect(cfg)
    finally:
        auth.use_provider(None)
    assert [kw["auth"].token for kw in seen] == ["token-1", "token-2"]
    assert all(kw["http_scheme"] == "https" and kw["verify"] == "/tmp/ca.crt" for kw in seen)
    assert all("user" not in kw for kw in seen)


def test_identity_failure_propagates_as_identity_error(monkeypatch):
    from idp.mcp import auth

    class _Down(_Provider):
        def token(self):
            raise auth.IdentityError("could not reach the identity provider", self.source)

    auth.use_provider(_Down())
    try:
        with pytest.raises(auth.IdentityError) as e:
            sql.query(load_config(), "SELECT 1")
        assert e.value.source == "http://kc/token"
    finally:
        auth.use_provider(None)


def test_tenant_has_data_binds_the_tenant(monkeypatch):
    conn = _fake(monkeypatch, rows=[("tenant_a",)], cols=["tenant_id"])
    assert sql.tenant_has_data(load_config(), "tenant_a")
    assert conn.calls[0][1] == ["tenant_a"] and "$partitions" not in conn.calls[0][0]
    _fake(monkeypatch, rows=[], cols=["tenant_id"])
    assert not sql.tenant_has_data(load_config(), "tenant_b")
