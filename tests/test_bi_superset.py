"""The Superset glue's identity rules, without Superset (REQ-BI-01, REQ-TEN-06).

``deploy/platform/superset/idp_superset.py`` decides who every Superset user and
connection is; its rules are plain functions, tested here. The Superset-side wiring
is verified on the cluster (the spike, ``make superset-smoke``).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

_PATH = Path(__file__).resolve().parents[1] / "deploy" / "platform" / "superset" / "idp_superset.py"
_spec = importlib.util.spec_from_file_location("idp_superset", _PATH)
idp_superset = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(idp_superset)

TENANTS = ["tenant_a", "tenant_b"]
ENV = {"IDP_TRINO_SUPERSET_PASSWORD": "trino-pw", "IDP_TRINO_CA_CERT": "/etc/idp/trino/tls.crt",
       "IDP_BI_PASSWORD_TENANT_A": "pw-a", "IDP_BI_PASSWORD_TENANT_B": "pw-b"}
TRINO = make_url("trino://superset@trino.idp-platform.svc:8443/lake")
GOLD = make_url("postgresql+psycopg2://bi@timescaledb.idp-platform.svc:5432/idp")


def _claims(**kw):
    base = {"sub": "123", "idp_principal": "alice@tenant_a", "tenant_id": "tenant_a",
            "email": "alice@tenant-a.example", "given_name": "Alice", "family_name": "Dev"}
    return {**base, **kw}


# --- REQ-BI-01: the signed-in user ---------------------------------------------
def test_user_is_the_principal():
    # SCN-BI-01.1
    u = idp_superset.user_from_claims(_claims(), TENANTS)
    assert u["username"] == "alice@tenant_a" and u["role_keys"] == ["tenant-user"]
    assert (u["email"], u["first_name"], u["last_name"]) == ("alice@tenant-a.example", "Alice", "Dev")


@pytest.mark.parametrize("claims", [
    {"tenant_id": None},                                        # no tenant claim
    {"tenant_id": "tenant_b"},                                  # principal and tenant disagree
    {"idp_principal": "alice@tenant_x", "tenant_id": "tenant_x"},   # not a configured tenant
    {"idp_principal": None},                                    # no principal
    {"idp_principal": "alice"},                                 # no tenant suffix
])
def test_inconsistent_identity_is_untenanted(claims):
    # SCN-BI-01.3: the user can sign in, as untenanted:<sub>, which gets no data
    u = idp_superset.user_from_claims(_claims(**claims), TENANTS)
    assert u["username"] == "untenanted:123" and u["role_keys"] == []    # no SQL Lab, no databases
    assert idp_superset.tenant_of(u["username"], TENANTS) is None


def test_admin_group_maps_to_role_keys():
    u = idp_superset.user_from_claims(_claims(groups=["/bi-admins", "/tenant_a", "other"]), TENANTS)
    assert u["role_keys"] == ["tenant-user", "bi-admins"]


@pytest.mark.parametrize("name,tenant", [("alice@tenant_a", "tenant_a"), ("mcp@tenant_b", "tenant_b"),
                                         ("alice@tenant_x", None), ("untenanted:1", None), ("", None), (None, None)])
def test_tenant_of(name, tenant):
    assert idp_superset.tenant_of(name, TENANTS) == tenant


# --- REQ-TEN-06: every connection's identity from the user ---------------------
def test_trino_impersonates_the_principal():
    # SCN-TEN-06.2 / REQ-BI-02: authenticated as superset, the URL user is the principal
    params = {"connect_args": {"user": "someone_else", "http_scheme": "http"}}
    uri, out = idp_superset.mutate_connection(TRINO, params, "alice@tenant_a", tenants=TENANTS, env=ENV)
    assert uri.username == "alice@tenant_a" and uri.password is None
    ca = out["connect_args"]
    assert ca["user"] == "alice@tenant_a"          # overrides what Superset's impersonation put there
    assert ca["http_scheme"] == "https" and ca["verify"] == "/etc/idp/trino/tls.crt"
    assert (ca["auth"]._username, ca["auth"]._password) == ("superset", "trino-pw")
    assert params == {"connect_args": {"user": "someone_else", "http_scheme": "http"}}   # input untouched


@pytest.mark.parametrize("user,role,pw", [("alice@tenant_a", "idp_bi_tenant_a", "pw-a"),
                                          ("bob@tenant_b", "idp_bi_tenant_b", "pw-b")])
def test_timescaledb_uses_the_tenant_role(user, role, pw):
    # SCN-TEN-06.2 / REQ-BI-03: whatever user the stored connection names
    uri, _ = idp_superset.mutate_connection(GOLD, {}, user, tenants=TENANTS, env=ENV)
    assert (uri.username, uri.password, uri.host, uri.database) == (role, pw, "timescaledb.idp-platform.svc", "idp")


@pytest.mark.parametrize("user", ["untenanted:123", "alice@tenant_x", "", None, "admin"])
@pytest.mark.parametrize("uri", [TRINO, GOLD])
def test_no_tenant_no_connection(user, uri):
    # SCN-BI-01.3, SCN-TEN-06.2
    with pytest.raises(idp_superset.ConnectionRefused, match="no tenant"):
        idp_superset.mutate_connection(uri, {}, user, tenants=TENANTS, env=ENV)


def test_other_databases_are_refused():
    with pytest.raises(idp_superset.ConnectionRefused, match="not allowed"):
        idp_superset.mutate_connection(make_url("sqlite:////tmp/x.db"), {}, "alice@tenant_a",
                                       tenants=TENANTS, env=ENV)


def test_missing_tenant_role_password_is_refused():
    env = {k: v for k, v in ENV.items() if k != "IDP_BI_PASSWORD_TENANT_B"}
    with pytest.raises(idp_superset.ConnectionRefused, match="IDP_BI_PASSWORD_TENANT_B"):
        idp_superset.mutate_connection(GOLD, {}, "bob@tenant_b", tenants=TENANTS, env=env)


def test_configured_tenants_from_env():
    assert idp_superset.configured_tenants({"IDP_TENANTS": " tenant_a, tenant_b ,"}) == TENANTS
    assert idp_superset.configured_tenants({}) == []


# --- superset_config.py, with stub Superset modules ---------------------------
def test_superset_config_settings(monkeypatch):
    import sys
    import types

    fab = types.ModuleType("flask_appbuilder.security.manager")
    fab.AUTH_OAUTH = 4
    sec = types.ModuleType("superset.security")
    sec.SupersetSecurityManager = type("SupersetSecurityManager", (), {})
    for name, mod in {"flask_appbuilder": types.ModuleType("flask_appbuilder"),
                      "flask_appbuilder.security": types.ModuleType("flask_appbuilder.security"),
                      "flask_appbuilder.security.manager": fab, "superset": types.ModuleType("superset"),
                      "superset.security": sec, "idp_superset": idp_superset}.items():
        monkeypatch.setitem(sys.modules, name, mod)
    monkeypatch.setenv("IDP_OAUTH_CLIENT_SECRET", "s")
    cfg = {}
    exec(compile((_PATH.parent / "superset_config.py").read_text(), "superset_config.py", "exec"), cfg)
    assert cfg["AUTH_TYPE"] == 4 and cfg["AUTH_USER_REGISTRATION_ROLE"] == "Gamma"
    assert cfg["AUTH_ROLES_MAPPING"] == {"tenant-user": ["sql_lab", "IdpData"], "bi-admins": ["Admin"]}
    assert cfg["AUTH_ROLES_SYNC_AT_LOGIN"] is True
    remote = cfg["OAUTH_PROVIDERS"][0]["remote_app"]
    assert remote["client_id"] == "idp-superset" and remote["client_secret"] == "s"
    assert remote["server_metadata_url"] == ("http://keycloak.idp-platform.svc:8080/realms/idp"
                                             "/.well-known/openid-configuration")
    assert cfg["DB_CONNECTION_MUTATOR"] is idp_superset.db_connection_mutator      # REQ-TEN-06
    assert cfg["DATA_CACHE_CONFIG"] == {"CACHE_TYPE": "NullCache"}                 # REQ-BI-04
    assert cfg["FEATURE_FLAGS"]["GLOBAL_ASYNC_QUERIES"] is False and cfg["PREVENT_UNSAFE_DB_CONNECTIONS"]


def test_content_importer_gets_the_data_free_metadata_identity():
    # REQ-BI-05: idp-content reads metadata as idp-content@meta / idp_bi_meta: no tenant's rows
    env = {**ENV, "IDP_BI_PASSWORD_META": "pw-meta"}
    uri, out = idp_superset.mutate_connection(TRINO, {}, "idp-content", tenants=TENANTS, env=env)
    assert uri.username == "idp-content@meta" and out["connect_args"]["user"] == "idp-content@meta"
    assert out["connect_args"]["auth"]._username == "superset"
    uri, _ = idp_superset.mutate_connection(GOLD, {}, "idp-content", tenants=TENANTS, env=env)
    assert (uri.username, uri.password) == ("idp_bi_meta", "pw-meta")


def test_a_signed_in_meta_principal_is_untenanted():
    # 'meta' is not a configured tenant, so a token claiming it gets nothing
    u = idp_superset.user_from_claims(_claims(idp_principal="alice@meta", tenant_id="meta"), TENANTS)
    assert u["username"] == "untenanted:123"
    with pytest.raises(idp_superset.ConnectionRefused):
        idp_superset.mutate_connection(GOLD, {}, "alice@meta", tenants=TENANTS, env=ENV)
