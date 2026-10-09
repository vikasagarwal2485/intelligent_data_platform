"""Cross-tenant leakage tests: each layer holds on its own (REQ-TEN-03, REQ-TEN-04).

Against the compose Trino stack (``make mcp-trino-test``), with real signed tokens
(``trino_stack``): the storage layer (Trino's row filter) is attacked with the
tools' own tenant filters removed, the identity layer (the MCP server's token) with
a query engine that doesn't filter at all, and authentication with forged tokens.
"""
from __future__ import annotations

import re

import pytest
import requests
import trino.exceptions as te

import trino_stack as stack
from trino_stack import as_tenant
from idp.mcp import sql, tools

pytestmark = pytest.mark.trino

OTHER = {"tenant_a": "tenant_b", "tenant_b": "tenant_a"}


def _tables(layer: str) -> list[str]:
    return [r[0] for r in stack.rows(stack.connect(password_of="admin"), f"SHOW TABLES FROM {layer}")]


def _denied(fn) -> str:
    with pytest.raises(te.TrinoUserError) as e:
        fn()
    assert e.value.error_name in ("PERMISSION_DENIED", "ACCESS_DENIED"), e.value
    return e.value.message


# --------------------------------------------------------------------------- #
# REQ-TEN-03 — row-level security                                              #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("tenant", stack.TENANTS)
def test_unfiltered_queries_see_one_tenant_in_every_table(trino_lake, tenant):
    # SCN-TEN-03.1, on every Silver and Gold table
    checked = 0
    for layer in ("silver", "gold"):
        for table in _tables(layer):
            got = stack.rows(stack.connect(principal=f"mcp@{tenant}"),
                             f"SELECT DISTINCT tenant_id FROM {layer}.{table}")
            assert {r[0] for r in got} <= {tenant}, f"{layer}.{table}: {got}"
            checked += 1
    assert checked >= 8


def test_asking_for_another_tenant_returns_nothing(trino_lake):
    # SCN-TEN-03.2
    for table in ("gold.credit_scores", "silver.dim_subscriber", "silver.fact_usage"):
        got = stack.rows(stack.connect(principal="mcp@tenant_a"),
                         f"SELECT count(*) FROM {table} WHERE tenant_id = 'tenant_b'")
        assert got == [[0]], table
    whole = stack.rows(stack.connect(password_of="admin"),
                       "SELECT count(*) FROM gold.credit_scores WHERE tenant_id = 'tenant_b'")
    assert whole[0][0] > 0                                  # the rows exist; they're filtered


@pytest.mark.parametrize("meta", ["partitions", "files", "snapshots", "manifests", "history", "properties"])
def test_metadata_tables_are_denied(trino_lake, meta):
    # SCN-TEN-03.3: row filters don't apply to Iceberg metadata tables
    for table in ("silver.dim_subscriber", "gold.credit_scores"):
        _denied(lambda: stack.rows(stack.connect(principal="mcp@tenant_a"),
                                   f'SELECT * FROM {table.split(".")[0]}."{table.split(".")[1]}${meta}"'))


def test_table_without_tenant_id_fails_closed(trino_lake):
    # SCN-TEN-03.4
    admin = lambda q: stack.rows(stack.connect(password_of="admin"), q)        # noqa: E731
    admin("CREATE TABLE IF NOT EXISTS silver.isolation_untagged AS SELECT 1 AS a")
    try:
        with pytest.raises(te.TrinoUserError) as e:
            stack.rows(stack.connect(principal="mcp@tenant_a"), "SELECT * FROM silver.isolation_untagged")
        assert "tenant_id" in e.value.message and "row filter" in e.value.message.lower()
    finally:
        admin("DROP TABLE IF EXISTS silver.isolation_untagged")


def test_unknown_principals_get_nothing(trino_lake):
    # SCN-TEN-03.5
    _denied(lambda: stack.rows(stack.connect(principal="someone"), "SELECT count(*) FROM silver.dim_subscriber"))
    got = stack.rows(stack.connect(principal="someone@tenant_x"), "SELECT count(*) FROM silver.dim_subscriber")
    assert got == [[0]]


def test_impersonation_is_filtered_and_bounded(trino_lake):
    # SCN-TEN-03.6
    got = stack.rows(stack.connect(password_of="superset", user="alice@tenant_b"),
                     "SELECT DISTINCT tenant_id FROM gold.fraud_scores")
    assert got == [["tenant_b"]]
    msg = _denied(lambda: stack.rows(stack.connect(password_of="superset", user="admin"), "SELECT 1"))
    assert "impersonate" in msg
    _denied(lambda: stack.rows(stack.connect(password_of="superset"), "SELECT count(*) FROM gold.fraud_scores"))


def test_statistics_are_computed_under_the_filter(trino_lake):
    # Statistics must not describe other tenants (design: spike results)
    def stats(conn, target):
        return {r[0]: r[1:] for r in stack.rows(conn, f"SHOW STATS FOR {target}")}
    mine = stats(stack.connect(principal="mcp@tenant_a"), "silver.dim_subscriber")
    only_a = stats(stack.connect(password_of="admin"),
                   "(SELECT * FROM silver.dim_subscriber WHERE tenant_id = 'tenant_a')")
    whole = stats(stack.connect(password_of="admin"), "silver.dim_subscriber")
    assert mine == only_a and mine != whole


# --------------------------------------------------------------------------- #
# REQ-TEN-04 — each layer holds on its own                                     #
# --------------------------------------------------------------------------- #
_PREDICATE = re.compile(r"\b(?:\w+\.)?tenant_id = \?")


class _Unfiltered:
    """Wraps ``sql.query``: removes each tool query's own tenant predicate (keeping
    the bound parameter) and records the tenants in every result."""

    def __init__(self, real):
        self.real, self.rewritten, self.tenants = real, 0, set()

    def __call__(self, cfg, text, params=()):
        new = _PREDICATE.sub("(? IS NOT NULL)", text)
        self.rewritten += new != text
        df = self.real(cfg, new, params)
        if "tenant_id" in df.columns:
            self.tenants |= set(df["tenant_id"].dropna().astype(str))
        return df


@pytest.mark.parametrize("tool", sorted(tools.TOOLS))
def test_storage_holds_without_the_tools_filter(trino_lake, monkeypatch, tool):
    # SCN-TEN-04.1: the tool's SQL no longer filters; Trino's row filter still does
    rec = _Unfiltered(sql.query)
    monkeypatch.setattr(sql, "query", rec)
    with as_tenant("tenant_a"):
        out = getattr(tools, tool)()
    assert rec.rewritten, "no tenant predicate was removed"
    assert "error" not in out, out                       # the gateway saw no foreign rows
    assert rec.tenants <= {"tenant_a"}


def test_the_unfiltered_queries_do_return_other_tenants_as_admin(trino_lake):
    # The control for SCN-TEN-04.1: without the row filter, the same rewrite leaks
    q = _PREDICATE.sub("(? IS NOT NULL)", "SELECT DISTINCT tenant_id FROM silver.dim_subscriber WHERE tenant_id = ?")
    cur = stack.connect(password_of="admin").cursor()
    cur.execute(q, ["tenant_a"])
    assert {r[0] for r in cur.fetchall()} == set(stack.TENANTS)


def test_identity_holds_without_the_storage_filter(trino_lake, monkeypatch):
    # SCN-TEN-04.2: an engine with no row filter (admin), and a tenant_a identity
    # asked for tenant_b: refused before any query
    monkeypatch.setattr(sql, "connect", lambda cfg: stack.connect(password_of="admin"))
    sent = []
    real = sql.query
    monkeypatch.setattr(sql, "query", lambda cfg, text, params=(): sent.append(text) or real(cfg, text, params))
    with as_tenant("tenant_a"):
        assert sql.query(trino_lake, "SELECT count(DISTINCT tenant_id) AS n FROM silver.dim_subscriber"
                         )["n"][0] == 2                    # this engine would leak
        sent.clear()
        for fn in tools.TOOLS.values():
            out = fn("tenant_b")
            assert out["available_tenants"] == ["tenant_a"] and "action_required" in out
    assert sent == []


@pytest.mark.parametrize("case", ["no token", "unknown key", "expired", "wrong audience",
                                  "wrong issuer", "no principal", "unsigned"])
def test_forged_or_missing_tokens_are_rejected(trino_lake, case):
    # SCN-TEN-04.3
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    import jwt

    other = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
    tokens = {
        "unknown key": stack.token("mcp@tenant_a", key=other),
        "expired": stack.token("mcp@tenant_a", ttl=-120),
        "wrong audience": stack.token("mcp@tenant_a", audience="idp-scoring-api"),
        "wrong issuer": stack.token("mcp@tenant_a", issuer="https://evil.invalid/realms/idp"),
        "no principal": stack.token(None, tenant="tenant_a"),
        "unsigned": jwt.encode({"aud": "trino", "iss": stack.ISSUER, "idp_principal": "mcp@tenant_a",
                                "tenant_id": "tenant_a"}, None, algorithm="none"),
    }
    conn = (stack.connect(user="mcp@tenant_a") if case == "no token"
            else stack.connect(raw_token=tokens[case]))
    with pytest.raises(te.HttpError) as e:
        stack.rows(conn, "SELECT count(*) FROM silver.dim_subscriber")
    assert "401" in str(e.value)


def test_credentials_only_over_https(trino_lake):
    # SCN-PLT-11.3 (a user name alone, a wrong password) and SCN-PLT-11.4 (HTTP)
    https = f"https://127.0.0.1:{stack.HTTPS_PORT}/v1/statement"
    bearer = {"Authorization": "Bearer " + stack.token("mcp@tenant_a")}

    def post(url, **kw):
        return requests.post(url, data="SELECT 1", verify=stack.ca_cert(), timeout=30, **kw).status_code

    assert post(https, headers={"X-Trino-User": "admin"}) == 401
    assert post(https, auth=("admin", "wrong")) == 401
    assert post("http://127.0.0.1:18080/v1/statement", headers=bearer) == 403
    assert post(https, headers=bearer) == 200
