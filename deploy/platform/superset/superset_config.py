# The platform's Superset settings (REQ-BI-01..04), appended by the operator after
# its own generated superset_config.py (SECRET_KEY, the metastore URI) and embedded
# in the Superset resource as spec.config by deploy/platform/superset/render.py.
# Secrets come from the environment (the superset-secrets and timescaledb-bi
# Secrets); nothing secret is in this file.
import os

from flask_appbuilder.security.manager import AUTH_OAUTH

import idp_superset

# --- Sign-in: Keycloak only (REQ-BI-01) ---------------------------------------
# The browser is sent to the issuer host (keycloak.idp-platform.svc:8080, reachable
# from the host through a hosts entry and `make superset-ui`); Superset's server side
# fetches the same URLs in-cluster.
_REALM = os.environ.get("IDP_KEYCLOAK_REALM_URL", "http://keycloak.idp-platform.svc:8080/realms/idp")
AUTH_TYPE = AUTH_OAUTH
OAUTH_PROVIDERS = [{
    "name": "keycloak",
    "icon": "fa-key",
    "token_key": "access_token",
    "remote_app": {
        "client_id": "idp-superset",
        "client_secret": os.environ["IDP_OAUTH_CLIENT_SECRET"],
        "server_metadata_url": f"{_REALM}/.well-known/openid-configuration",
        "client_kwargs": {"scope": "openid email profile"},
    },
}]
# Roles: every user Gamma (view dashboards and charts); a user with a tenant also
# sql_lab and IdpData (access to the two databases, created with the content by
# content.py); the realm group bi-admins Admin. A user without a tenant keeps Gamma
# only, and the connection mutator refuses them anyway (SCN-BI-01.3).
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Gamma"
AUTH_ROLES_MAPPING = {
    idp_superset.TENANT_USER: ["sql_lab", "IdpData"],
    idp_superset.ADMIN_GROUP: ["Admin"],
}
AUTH_ROLES_SYNC_AT_LOGIN = True
CUSTOM_SECURITY_MANAGER = idp_superset.security_manager_class()

# --- Every connection's identity from the signed-in user (REQ-BI-02/03) ----------
DB_CONNECTION_MUTATOR = idp_superset.db_connection_mutator

# --- No cross-tenant results (REQ-BI-04) -----------------------------------------
# Chart data is never cached; UI state (filters, explore form data) is per user and
# kept in the metadata database. No results backend: queries run synchronously.
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
FILTER_STATE_CACHE_CONFIG = {"CACHE_TYPE": "SupersetMetastoreCache", "CACHE_DEFAULT_TIMEOUT": 86400}
EXPLORE_FORM_DATA_CACHE_CONFIG = {"CACHE_TYPE": "SupersetMetastoreCache", "CACHE_DEFAULT_TIMEOUT": 86400}

# --- Read-only BI ---------------------------------------------------------------
PREVENT_UNSAFE_DB_CONNECTIONS = True
SQLLAB_CTAS_NO_LIMIT = False
FEATURE_FLAGS = {"ALERT_REPORTS": False, "THUMBNAILS": False, "GLOBAL_ASYNC_QUERIES": False}
