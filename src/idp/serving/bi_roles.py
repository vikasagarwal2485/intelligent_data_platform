"""Gold isolation for BI readers in the serving database (REQ-BI-03, REQ-TEN-06).

Superset reads the ``gold.*`` serving tables as the signed-in user's **tenant role**:
one login role per configured tenant, ``idp_bi_<tenant>``, each a member of the group
``idp_bi_readers`` and of nothing else. One row-level-security policy per table, for
the group, returns only the rows of the role's own tenant:

    tenant_id = regexp_replace(current_user, '^idp_bi_', '')

so a new tenant needs a role, never a policy. ``postgres`` (publish, the scoring
API) is a superuser and bypasses row-level security.

:func:`apply_table_controls` enables RLS, (re)creates the policy and grants
``SELECT`` on one table. ``timescale.swap_in`` calls it inside the publish
transaction, because each publish replaces the table and would otherwise drop them
(REQ-SRV-01). :func:`provision` (``idp bi-roles``) creates the roles, grants the
serving database and schema, revokes ``CONNECT`` on every other database of the
server from ``PUBLIC`` (catalog, MLflow, Airflow, Nessie, Superset, and any added
later), and applies the controls to the existing tables.
"""
from __future__ import annotations

import os
import re
from typing import Any, Iterable, Mapping

GROUP = "idp_bi_readers"
ROLE_PREFIX = "idp_bi_"
POLICY = "bi_tenant_rows"
# The metadata identity (``idp_bi_meta``): tooling such as the BI content import reads
# table metadata through it; no row carries tenant_id 'meta', so it reads no data.
META = "meta"
_TENANT = re.compile(r"^[a-z0-9_]+$")


def role_name(tenant: str) -> str:
    """The tenant's login role; tenant ids are plain identifiers."""
    if not _TENANT.match(tenant):
        raise ValueError(f"tenant id {tenant!r} is not a plain identifier ([a-z0-9_]+)")
    return ROLE_PREFIX + tenant


def password_env(tenant: str) -> str:
    return "IDP_BI_PASSWORD_" + tenant.upper()


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _qualified(table: str, schema: str | None) -> str:
    return f"{_q(schema)}.{_q(table)}" if schema else _q(table)


def ensure_group(conn) -> None:
    """Create the readers' group role if it doesn't exist (serialized, so parallel
    publishers don't race on it)."""
    from sqlalchemy import text

    conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": GROUP})
    exists = conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": GROUP}).first()
    if not exists:
        conn.execute(text(f"CREATE ROLE {_q(GROUP)} NOLOGIN"))


def apply_table_controls(conn, table: str, schema: str | None) -> None:
    """Row-level security, the tenant policy and the readers' ``SELECT`` on one
    table, in the caller's transaction (REQ-SRV-01, decision 6). Postgres only."""
    from sqlalchemy import text

    ensure_group(conn)
    t = _qualified(table, schema)
    conn.execute(text(f"ALTER TABLE {t} ENABLE ROW LEVEL SECURITY"))
    conn.execute(text(f"DROP POLICY IF EXISTS {_q(POLICY)} ON {t}"))
    conn.execute(text(
        f"CREATE POLICY {_q(POLICY)} ON {t} FOR SELECT TO {_q(GROUP)} "
        f"USING (tenant_id = regexp_replace(current_user::text, '^{ROLE_PREFIX}', ''))"))
    conn.execute(text(f"GRANT SELECT ON {t} TO {_q(GROUP)}"))


def provision(engine, tenants: Iterable[str], passwords: Mapping[str, str], *, database: str,
              schema: str = "gold", other_databases: Iterable[str] | None = None) -> dict[str, Any]:
    """Create or update the readers' group and one login role per tenant, grant the
    serving database and schema, revoke ``CONNECT`` from ``PUBLIC`` on the other
    databases (``other_databases``, or by default every database on the server
    except the serving one and the templates), and apply the table controls to every
    table already in ``schema``. Idempotent. Returns what it did."""
    from sqlalchemy import text

    tenants = list(tenants)
    if META in tenants:
        raise ValueError(f"tenant id {META!r} is reserved for the metadata identity")
    tenants = tenants + [META]
    roles = {t: role_name(t) for t in tenants}
    missing = [t for t in tenants if not passwords.get(t)]
    if missing:
        raise ValueError(f"no password for tenant role(s) of {missing} "
                         f"(set {', '.join(password_env(t) for t in missing)})")
    with engine.begin() as conn:
        ensure_group(conn)
        for tenant, role in roles.items():
            exists = conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": role}).first()
            verb = "ALTER" if exists else "CREATE"
            conn.execute(text(f"{verb} ROLE {_q(role)} LOGIN INHERIT NOSUPERUSER NOCREATEDB "
                              f"NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :pw"),
                         {"pw": passwords[tenant]})
            conn.execute(text(f"GRANT {_q(GROUP)} TO {_q(role)}"))
            # a tenant role belongs to the readers' group only
            for (other,) in conn.execute(text(
                    "SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid "
                    "JOIN pg_roles u ON u.oid = m.member WHERE u.rolname = :r AND g.rolname <> :g"),
                    {"r": role, "g": GROUP}).fetchall():
                conn.execute(text(f"REVOKE {_q(other)} FROM {_q(role)}"))
        conn.execute(text(f"GRANT CONNECT ON DATABASE {_q(database)} TO {_q(GROUP)}"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)}"))
        conn.execute(text(f"GRANT USAGE ON SCHEMA {_q(schema)} TO {_q(GROUP)}"))
        present = [r[0] for r in conn.execute(text(
            "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname")).fetchall()]
        wanted = present if other_databases is None else list(other_databases)
        revoked = [d for d in wanted if d in present and d != database]
        for db in revoked:
            conn.execute(text(f"REVOKE CONNECT ON DATABASE {_q(db)} FROM PUBLIC"))
        tables = [r[0] for r in conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = :s ORDER BY tablename"), {"s": schema}).fetchall()]
        for table in tables:
            apply_table_controls(conn, table, schema)
    return {"group": GROUP, "roles": sorted(roles.values()), "tables": tables, "revoked_connect": revoked}


def passwords_from_env(tenants: Iterable[str], env: Mapping[str, str] | None = None) -> dict[str, str]:
    """The roles' passwords (the configured tenants and the metadata identity)."""
    env = os.environ if env is None else env
    return {t: env.get(password_env(t), "") for t in [*tenants, META]}
