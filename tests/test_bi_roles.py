"""Gold isolation for BI readers in a real Postgres (REQ-BI-03, REQ-TEN-06, REQ-SRV-01).

Uses the compose stack's Postgres (``make trino-stack``; host port 15432) with two
scratch databases: one serving database and one "other" database whose ``CONNECT``
is revoked. Runs with ``pytest -m trino`` (``make mcp-trino-test``).
"""
from __future__ import annotations

import threading

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from idp.serving import bi_roles, timescale

pytestmark = pytest.mark.trino

ADMIN = "postgresql+psycopg2://postgres:idp-dev-password@127.0.0.1:15432/{db}"
SERVING, OTHER = "idp_bi_test", "idp_bi_test_other"
TENANTS = ["tenant_a", "tenant_b"]
PASSWORDS = {"tenant_a": "pw-a", "tenant_b": "pw-b", "tenant_c": "pw-c", "meta": "pw-meta"}


def _engine(db: str, user: str = "postgres", password: str = "idp-dev-password"):
    return create_engine(f"postgresql+psycopg2://{user}:{password}@127.0.0.1:15432/{db}")


def _as(tenant: str, db: str = SERVING):
    return _engine(db, bi_roles.role_name(tenant), PASSWORDS[tenant])


def _scores(tenants, n=5, run=0) -> pd.DataFrame:
    return pd.DataFrame([{"tenant_id": t, "service_id": f"tok_{t}_{i}", "pd": 0.1 * i, "run": run}
                         for t in tenants for i in range(n)])


@pytest.fixture(scope="module")
def serving():
    try:
        admin = _engine("postgres").execution_options(isolation_level="AUTOCOMMIT")
        with admin.connect() as c:
            for db in (SERVING, OTHER):
                c.execute(text(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)'))
            # roles are server-wide: start without any left by an earlier run
            for (role,) in c.execute(text("SELECT rolname FROM pg_roles WHERE rolname LIKE 'idp\\_bi\\_%' "
                                          "ORDER BY rolname = 'idp_bi_readers'")).fetchall():
                c.execute(text(f'DROP ROLE "{role}"'))
            for db in (SERVING, OTHER):
                c.execute(text(f'CREATE DATABASE "{db}"'))
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"compose Postgres not reachable ({e}); `make trino-stack` starts it")
    eng = _engine(SERVING)
    timescale.publish_frame(eng, "credit_scores", _scores(TENANTS), schema="gold")
    timescale.publish_frame(eng, "fraud_scores", _scores(TENANTS), schema="gold")
    done = bi_roles.provision(eng, TENANTS, PASSWORDS, database=SERVING, other_databases=[OTHER])
    yield eng, done
    eng.dispose()


def _rows(engine, sql: str) -> list:
    with engine.connect() as c:
        return [tuple(r) for r in c.execute(text(sql)).fetchall()]


def test_provision_reports_roles_and_tables(serving):
    _, done = serving
    assert done["roles"] == ["idp_bi_meta", "idp_bi_tenant_a", "idp_bi_tenant_b"]
    assert done["tables"] == ["credit_scores", "fraud_scores"] and done["revoked_connect"] == [OTHER]


@pytest.mark.parametrize("tenant", TENANTS)
def test_policy_holds_without_a_filter(serving, tenant):
    # SCN-TEN-06.1 and SCN-BI-03.1, on every serving table
    for table in ("credit_scores", "fraud_scores"):
        assert _rows(_as(tenant), f"SELECT DISTINCT tenant_id FROM gold.{table}") == [(tenant,)]


def test_the_role_cannot_escape(serving):
    # SCN-BI-03.2
    a = _as("tenant_a")
    assert _rows(a, "SELECT count(*) FROM gold.fraud_scores WHERE tenant_id = 'tenant_b'") == [(0,)]
    with pytest.raises(DBAPIError, match="permission denied"):
        _rows(a, "SET ROLE idp_bi_tenant_b")
    with pytest.raises(DBAPIError, match="permission denied"):
        _rows(_as("tenant_a", OTHER), "SELECT 1")                     # CONNECT revoked from PUBLIC
    with pytest.raises(DBAPIError, match="permission denied"):
        _rows(a, "INSERT INTO gold.credit_scores (tenant_id, service_id) VALUES ('tenant_a', 'x')")
    with pytest.raises(DBAPIError, match="must be owner|permission denied"):
        _rows(a, "ALTER TABLE gold.credit_scores DISABLE ROW LEVEL SECURITY")


def test_tenant_roles_belong_to_the_readers_group_only(serving):
    eng, _ = serving
    with eng.begin() as c:                                  # a stray membership is removed on re-provision
        c.execute(text("GRANT idp_bi_tenant_b TO idp_bi_tenant_a"))
    bi_roles.provision(eng, TENANTS, PASSWORDS, database=SERVING, other_databases=[OTHER])
    got = _rows(eng, "SELECT u.rolname, g.rolname FROM pg_auth_members m "
                     "JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member "
                     "WHERE u.rolname LIKE 'idp_bi_tenant_%' ORDER BY 1")
    assert got == [("idp_bi_tenant_a", "idp_bi_readers"), ("idp_bi_tenant_b", "idp_bi_readers")]
    assert _rows(eng, "SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid "
                      "JOIN pg_roles u ON u.oid = m.member WHERE u.rolname = 'idp_bi_meta'") == [("idp_bi_readers",)]


def test_adding_a_tenant_needs_only_its_role(serving):
    # SCN-BI-03.3: no policy is edited; the new role reads only its rows
    eng, _ = serving
    policies = lambda: _rows(eng, "SELECT tablename, qual FROM pg_policies WHERE schemaname = 'gold' ORDER BY 1")  # noqa: E731
    before = policies()
    timescale.publish_frame(eng, "credit_scores", _scores(TENANTS + ["tenant_c"]), schema="gold")
    bi_roles.provision(eng, TENANTS + ["tenant_c"], PASSWORDS, database=SERVING, other_databases=[OTHER])
    assert policies() == before
    assert _rows(_as("tenant_c"), "SELECT DISTINCT tenant_id FROM gold.credit_scores") == [("tenant_c",)]


def test_access_controls_survive_a_publish(serving):
    # SCN-SRV-01.6: a tenant_a reader in a loop while the table is replaced, repeatedly
    eng, _ = serving
    reader, errors, seen, stop = _as("tenant_a"), [], set(), threading.Event()

    def read():
        while not stop.is_set():
            try:
                for (t,) in _rows(reader, "SELECT tenant_id FROM gold.fraud_scores"):
                    seen.add(t)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

    th = threading.Thread(target=read)
    th.start()
    try:
        for run in range(1, 6):
            timescale.publish_frame(eng, "fraud_scores", _scores(TENANTS, run=run), schema="gold")
    finally:
        stop.set()
        th.join()
    assert errors == [] and seen == {"tenant_a"}
    assert _rows(_as("tenant_a"), "SELECT DISTINCT run FROM gold.fraud_scores") == [(5,)]


def test_superuser_still_reads_everything(serving):
    # the publish stage and the scoring API connect as postgres: unaffected
    eng, _ = serving
    assert {r[0] for r in _rows(eng, "SELECT DISTINCT tenant_id FROM gold.credit_scores")} >= set(TENANTS)


def test_missing_password_is_refused(serving):
    eng, _ = serving
    with pytest.raises(ValueError, match="IDP_BI_PASSWORD_TENANT_B"):
        bi_roles.provision(eng, TENANTS, {"tenant_a": "x", "meta": "y"}, database=SERVING)
    with pytest.raises(ValueError, match="IDP_BI_PASSWORD_META"):
        bi_roles.provision(eng, TENANTS, {"tenant_a": "x", "tenant_b": "y"}, database=SERVING)


def test_metadata_identity_reads_no_rows(serving):
    # REQ-BI-05: the content tooling's role sees the tables, never a tenant's rows
    meta = _engine(SERVING, "idp_bi_meta", PASSWORDS["meta"])
    for table in ("credit_scores", "fraud_scores"):
        assert _rows(meta, f"SELECT count(*) FROM gold.{table}") == [(0,)]
    assert {r[0] for r in _rows(meta, "SELECT tablename FROM pg_tables WHERE schemaname = 'gold'")} >= {"credit_scores"}


def test_meta_is_a_reserved_tenant_id(serving):
    eng, _ = serving
    with pytest.raises(ValueError, match="reserved"):
        bi_roles.provision(eng, ["tenant_a", "meta"], PASSWORDS, database=SERVING)


def test_role_names_are_plain_identifiers():
    assert bi_roles.role_name("tenant_a") == "idp_bi_tenant_a"
    with pytest.raises(ValueError):
        bi_roles.role_name('x"; DROP ROLE postgres; --')
