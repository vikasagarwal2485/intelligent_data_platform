"""The platform's Superset glue (REQ-BI-01..04, REQ-TEN-06).

Everything that decides *who* a query runs as lives here, as plain functions with
no Superset import, so it is unit-tested without Superset
(``tests/test_bi_superset.py``). ``superset_config.py`` wires them in:

- :func:`user_from_claims` maps the Keycloak claims of a sign-in to the Superset
  user: the user name is the token's ``idp_principal`` (``<name>@<tenant>``) when it
  is consistent with ``tenant_id`` and a configured tenant, else
  ``untenanted:<sub>``, which no database connection accepts (SCN-BI-01.3).
- :func:`mutate_connection` (``DB_CONNECTION_MUTATOR``) sets the identity of every
  database connection from the signed-in user, never from the query:
  Trino runs as ``superset`` (password) impersonating the user's principal, so
  Trino's row filter applies (REQ-BI-02); TimescaleDB runs as the user's tenant
  login role ``idp_bi_<tenant>``, whose row-level-security policy applies
  (REQ-BI-03). Any other database, or a user without a tenant, is refused.
- The technical user ``idp-content`` (content import; it can't sign in) gets the
  **metadata identity** ``meta``: Trino principal ``idp-content@meta`` and role
  ``idp_bi_meta``. Both read table metadata but match no rows, since no data has
  ``tenant_id = 'meta'`` (REQ-BI-05).
"""
from __future__ import annotations

import os
import re
from typing import Any, Iterable, Mapping

ADMIN_GROUP = "bi-admins"
TENANT_USER = "tenant-user"          # role key of every user with a tenant
UNTENANTED = "untenanted:"
ROLE_PREFIX = "idp_bi_"
METADATA_USER = "idp-content"       # the content importer
META = "meta"                       # its reserved, data-free tenant name
_PRINCIPAL = re.compile(r"^([a-z0-9._-]+)@([a-z0-9_]+)$")


class ConnectionRefused(Exception):
    """No connection for this user and database (no tenant, or an unknown database)."""


def configured_tenants(env: Mapping[str, str] | None = None) -> list[str]:
    """The configured tenants, from ``IDP_TENANTS`` (comma-separated)."""
    env = os.environ if env is None else env
    return [t.strip() for t in env.get("IDP_TENANTS", "").split(",") if t.strip()]


def tenant_of(username: str | None, tenants: Iterable[str]) -> str | None:
    """The tenant of a Superset user name: the suffix of ``<name>@<tenant>`` when it
    is a configured tenant, else ``None``."""
    m = _PRINCIPAL.match(username or "")
    if not m or m.group(2) not in set(tenants):
        return None
    return m.group(2)


def user_from_claims(claims: Mapping[str, Any], tenants: Iterable[str],
                     admin_group: str = ADMIN_GROUP) -> dict[str, Any]:
    """The Superset user for a Keycloak sign-in (REQ-BI-01).

    The user name is ``idp_principal`` when its suffix equals ``tenant_id`` and is a
    configured tenant; otherwise ``untenanted:<sub>``. ``role_keys`` feeds
    ``AUTH_ROLES_MAPPING``: ``tenant-user`` for a user with a tenant (SQL Lab and the
    two databases), ``bi-admins`` for that realm group (Admin). Every user also gets
    the registration role (Gamma: view dashboards and charts)."""
    tenants = list(tenants)
    principal = str(claims.get("idp_principal") or "")
    tenant = str(claims.get("tenant_id") or "")
    ok = bool(tenant) and tenant_of(principal, tenants) == tenant
    username = principal if ok else f"{UNTENANTED}{claims.get('sub') or claims.get('preferred_username') or 'unknown'}"
    groups = [str(g).rsplit("/", 1)[-1] for g in (claims.get("groups") or [])]
    return {
        "username": username,
        "email": claims.get("email") or "",
        "first_name": claims.get("given_name") or "",
        "last_name": claims.get("family_name") or "",
        "role_keys": ([TENANT_USER] if ok else []) + [g for g in groups if g == admin_group],
    }


def role_name(tenant: str) -> str:
    return ROLE_PREFIX + tenant


def password_env(tenant: str) -> str:
    return "IDP_BI_PASSWORD_" + tenant.upper()


def mutate_connection(uri, params: dict, username: str | None, *, tenants: Iterable[str],
                      env: Mapping[str, str] | None = None):
    """The connection for ``username`` (``DB_CONNECTION_MUTATOR``, REQ-TEN-06).

    ``uri`` is a SQLAlchemy ``URL``; ``params`` the engine keyword arguments. Only the
    signed-in user's name decides the identity; nothing in the query or chart reaches
    this function. Returns ``(uri, params)``; raises :class:`ConnectionRefused`."""
    env = os.environ if env is None else env
    if username == METADATA_USER:
        tenant, principal = META, f"{METADATA_USER}@{META}"
    else:
        tenant, principal = tenant_of(username, tenants), username
    if tenant is None:
        raise ConnectionRefused(f"user {username!r} has no tenant: no data access")
    backend = uri.get_backend_name()
    params = dict(params or {})
    if backend == "trino":
        from trino.auth import BasicAuthentication

        connect_args = dict(params.get("connect_args") or {})
        connect_args.update({
            # the impersonated user: set here, since Superset's own impersonation puts the
            # signed-in user name into connect_args and the Trino driver prefers it to the URL
            "user": principal,
            "http_scheme": "https",
            "verify": env.get("IDP_TRINO_CA_CERT") or True,
            # authenticated as the service user; the URL's user is impersonated
            "auth": BasicAuthentication("superset", env["IDP_TRINO_SUPERSET_PASSWORD"]),
        })
        params["connect_args"] = connect_args
        return uri.set(username=principal, password=None), params
    if backend == "postgresql":
        password = env.get(password_env(tenant))
        if not password:
            raise ConnectionRefused(f"no password for {role_name(tenant)} ({password_env(tenant)})")
        return uri.set(username=role_name(tenant), password=password), params
    raise ConnectionRefused(f"database backend {backend!r} is not allowed for BI users")


# --------------------------------------------------------------------------- #
# Superset glue (imported only inside Superset)                                #
# --------------------------------------------------------------------------- #
def db_connection_mutator(uri, params, username, security_manager, source):  # noqa: ARG001
    """``DB_CONNECTION_MUTATOR``: Superset's hook around every engine it creates."""
    return mutate_connection(uri, params, username, tenants=configured_tenants())


def security_manager_class():
    """A ``SupersetSecurityManager`` whose OAuth user info comes from the Keycloak
    claims through :func:`user_from_claims`."""
    from superset.security import SupersetSecurityManager

    class IdpSecurityManager(SupersetSecurityManager):
        def oauth_user_info(self, provider, response=None):
            if provider != "keycloak":
                return {}
            claims = dict((response or {}).get("userinfo") or {})
            if not claims:
                claims = self.appbuilder.sm.oauth_remotes[provider].userinfo(token=response)
            return user_from_claims(claims, configured_tenants())

    return IdpSecurityManager
