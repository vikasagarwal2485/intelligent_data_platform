"""The Keycloak realm, rendered from the configured tenants (REQ-TEN-01, REQ-TEN-02).

``deploy/platform/keycloak/idp-realm.base.json`` holds everything shared: the realm
settings, the user profile, the scoring-API, Superset and dev-CLI clients, and the
dev users. :func:`render` adds, for each configured tenant:

- a group named after the tenant, for its users;
- its scoring-API machine client ``idp-<tenant>`` (``tenant_a`` -> ``idp-tenant-a``),
  whose tokens carry ``tenant_id`` and the scoring API's audience;
- its MCP service client ``idp-mcp-<tenant>``, whose tokens carry ``tenant_id``,
  ``idp_principal = mcp@<tenant>`` and the audience ``trino``.

The claims are hard-coded on the clients, so a client can't ask for another tenant.
Client secrets come from ``IDP_KC_SECRET_<CLIENT>`` (the client id upper-cased, other
characters as ``_``, e.g. ``IDP_KC_SECRET_IDP_MCP_TENANT_A``), else a documented dev
default. ``idp keycloak-realm`` writes ``idp-realm.json``; a test checks the
committed file equals the rendering (with no secret overrides).
"""
from __future__ import annotations

import copy
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

KEYCLOAK_DIR = Path(__file__).resolve().parents[3] / "deploy" / "platform" / "keycloak"
BASE_FILE = KEYCLOAK_DIR / "idp-realm.base.json"
REALM_FILE = KEYCLOAK_DIR / "idp-realm.json"

TRINO_AUDIENCE = "trino"
SCORING_AUDIENCE = "idp-scoring-api"
MCP_PRINCIPAL_NAME = "mcp"


def scoring_client_id(tenant: str) -> str:
    return "idp-" + tenant.replace("_", "-")


def mcp_client_id(tenant: str) -> str:
    return f"idp-mcp-{tenant}"


def mcp_principal(tenant: str) -> str:
    return f"{MCP_PRINCIPAL_NAME}@{tenant}"


def secret_env(client_id: str) -> str:
    return "IDP_KC_SECRET_" + re.sub(r"[^A-Z0-9]", "_", client_id.upper())


def _secret(client_id: str, default: str, env: Mapping[str, str]) -> str:
    return env.get(secret_env(client_id)) or default


def _audience(audience: str) -> dict[str, Any]:
    return {"name": f"audience: {audience}", "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper", "consentRequired": False,
            "config": {"included.client.audience": audience, "id.token.claim": "false",
                       "access.token.claim": "true", "introspection.token.claim": "true"}}


def _hardcoded(claim: str, value: str) -> dict[str, Any]:
    return {"name": f"{claim} (hard-coded)", "protocol": "openid-connect",
            "protocolMapper": "oidc-hardcoded-claim-mapper", "consentRequired": False,
            "config": {"claim.name": claim, "claim.value": value, "jsonType.label": "String",
                       "access.token.claim": "true", "id.token.claim": "false",
                       "introspection.token.claim": "true", "userinfo.token.claim": "false"}}


def _machine_client(client_id: str, name: str, secret: str, mappers: list[dict]) -> dict[str, Any]:
    return {"clientId": client_id, "name": name, "enabled": True, "publicClient": False,
            "clientAuthenticatorType": "client-secret", "secret": secret,
            "serviceAccountsEnabled": True, "standardFlowEnabled": False,
            "directAccessGrantsEnabled": False, "implicitFlowEnabled": False,
            "protocol": "openid-connect", "protocolMappers": mappers}


def tenant_clients(tenant: str, env: Mapping[str, str]) -> list[dict[str, Any]]:
    """The two machine clients of one tenant."""
    scoring, mcp = scoring_client_id(tenant), mcp_client_id(tenant)
    slug = tenant.replace("_", "-")
    return [
        _machine_client(scoring, f"Machine caller for {tenant} (scoring API)",
                        _secret(scoring, f"{slug}-dev-secret", env),
                        [_audience(SCORING_AUDIENCE), _hardcoded("tenant_id", tenant)]),
        _machine_client(mcp, f"MCP analytics server for {tenant} (Trino)",
                        _secret(mcp, f"mcp-{slug}-dev-secret", env),
                        [_audience(TRINO_AUDIENCE), _hardcoded("tenant_id", tenant),
                         _hardcoded("idp_principal", mcp_principal(tenant))]),
    ]


def render(tenants: list[str], base: dict[str, Any] | None = None,
           env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The realm for ``tenants``: the base plus each tenant's group and clients."""
    env = os.environ if env is None else env
    realm = copy.deepcopy(base if base is not None else json.loads(BASE_FILE.read_text()))
    realm.pop("_comment", None)
    taken = {c["clientId"] for c in realm.get("clients", [])}
    clients = list(realm.get("clients", []))
    for tenant in tenants:
        for client in tenant_clients(tenant, env):
            if client["clientId"] in taken:
                raise ValueError(f"client {client['clientId']!r} is already in the realm base")
            clients.append(client)
    realm["clients"] = clients
    realm["groups"] = list(realm.get("groups", [])) + [
        {"name": t, "attributes": {"tenant_id": [t]}} for t in tenants]
    known = set(tenants)
    for user in realm.get("users", []):
        tenant = (user.get("attributes", {}).get("tenant_id") or [None])[0]
        principal = (user.get("attributes", {}).get("idp_principal") or [None])[0]
        if tenant not in known:
            raise ValueError(f"user {user['username']!r} has tenant {tenant!r}, not a configured tenant")
        if principal != f"{user['username']}@{tenant}":
            raise ValueError(f"user {user['username']!r}: idp_principal {principal!r} must be "
                             f"'{user['username']}@{tenant}'")
    return realm


def dumps(realm: dict[str, Any]) -> str:
    return json.dumps(realm, indent=2) + "\n"
