"""The Keycloak realm rendered from the configured tenants (REQ-TEN-01, REQ-TEN-02)."""
from __future__ import annotations

import json

import pytest

from idp.common.config import load_config
from idp.identity import realm


def _tenants() -> list[str]:
    return [t.id for t in load_config().tenants]


def _clients(r: dict) -> dict[str, dict]:
    return {c["clientId"]: c for c in r["clients"]}


def _claims(client: dict) -> dict[str, str]:
    """Hard-coded claims and audiences a client's tokens carry."""
    out = {}
    for m in client["protocolMappers"]:
        cfg = m["config"]
        if m["protocolMapper"] == "oidc-hardcoded-claim-mapper":
            out[cfg["claim.name"]] = cfg["claim.value"]
        elif m["protocolMapper"] == "oidc-audience-mapper":
            out.setdefault("aud", []).append(cfg["included.client.audience"])
    return out


def test_committed_realm_matches_the_configured_tenants():
    # SCN-TEN-02.1 (rendered without secret overrides, as committed)
    assert realm.REALM_FILE.read_text() == realm.dumps(realm.render(_tenants(), env={}))


def test_adding_a_tenant_adds_only_its_group_and_clients():
    # SCN-TEN-02.2, at render level
    before = realm.render(_tenants(), env={})
    after = realm.render(_tenants() + ["tenant_c"], env={})
    assert set(_clients(after)) - set(_clients(before)) == {"idp-tenant-c", "idp-mcp-tenant_c"}
    assert [g["name"] for g in after["groups"]][-1] == "tenant_c"
    assert {k: v for k, v in after.items() if k not in ("clients", "groups")} == \
           {k: v for k, v in before.items() if k not in ("clients", "groups")}


@pytest.mark.parametrize("tenant", ["tenant_a", "tenant_b"])
def test_mcp_client_tokens_carry_principal_tenant_and_trino_audience(tenant):
    # SCN-TEN-01.2: claims are hard-coded on the client, so it can't ask for another
    mcp = _clients(realm.render(_tenants(), env={}))[f"idp-mcp-{tenant}"]
    assert mcp["serviceAccountsEnabled"] and not mcp["publicClient"]
    assert not mcp["standardFlowEnabled"] and not mcp["directAccessGrantsEnabled"]
    assert _claims(mcp) == {"aud": ["trino"], "tenant_id": tenant, "idp_principal": f"mcp@{tenant}"}


def test_scoring_clients_keep_their_audience_and_tenant():
    clients = _clients(realm.render(_tenants(), env={}))
    assert _claims(clients["idp-tenant-a"]) == {"aud": ["idp-scoring-api"], "tenant_id": "tenant_a"}
    assert clients["idp-tenant-a"]["secret"] == "tenant-a-dev-secret"


def test_users_carry_a_consistent_principal_and_group():
    # SCN-TEN-01.1: user tokens get both claims from admin-set attributes
    r = realm.render(_tenants(), env={})
    for u in r["users"]:
        tenant = u["attributes"]["tenant_id"][0]
        assert u["attributes"]["idp_principal"] == [f"{u['username']}@{tenant}"]
        assert u["groups"][0] == f"/{tenant}" and set(u["groups"][1:]) <= {"/bi-admins"}
    cli = _clients(r)["idp-dev-cli"]
    attrs = {m["config"]["claim.name"] for m in cli["protocolMappers"]
             if m["protocolMapper"] == "oidc-usermodel-attribute-mapper"}
    assert attrs == {"tenant_id", "idp_principal"} and "trino" in _claims(cli)["aud"]


def test_users_cannot_edit_their_tenant_claims():
    # SCN-TEN-01.3: the declarative user profile lets only admins edit them
    r = realm.render(_tenants(), env={})
    up = r["components"]["org.keycloak.userprofile.UserProfileProvider"][0]["config"]
    attrs = {a["name"]: a for a in json.loads(up["kc.user.profile.config"][0])["attributes"]}
    for name in ("tenant_id", "idp_principal"):
        assert attrs[name]["permissions"]["edit"] == ["admin"]


def test_inconsistent_user_principal_is_refused():
    base = json.loads(realm.BASE_FILE.read_text())
    base["users"][0]["attributes"]["idp_principal"] = ["alice@tenant_b"]
    with pytest.raises(ValueError, match="idp_principal"):
        realm.render(_tenants(), base=base, env={})


def test_secrets_come_from_the_environment_when_set():
    env = {realm.secret_env("idp-mcp-tenant_a"): "from-env"}
    assert realm.secret_env("idp-mcp-tenant_a") == "IDP_KC_SECRET_IDP_MCP_TENANT_A"
    assert _clients(realm.render(_tenants(), env=env))["idp-mcp-tenant_a"]["secret"] == "from-env"


def test_cli_check(tmp_path, capsys):
    from idp.cli import main
    out = tmp_path / "realm.json"
    assert main(["keycloak-realm", "--out", str(out)]) == 0
    assert main(["keycloak-realm", "--out", str(out), "--check"]) == 0
    out.write_text("{}")
    assert main(["keycloak-realm", "--out", str(out), "--check"]) == 1


def test_superset_client_signs_in_with_tenant_principal_and_groups():
    # REQ-BI-01: the BI login client carries the principal, the tenant and the groups
    # (bi-admins -> Superset Admin), and redirects only to Superset
    r = realm.render(_tenants(), env={})
    c = _clients(r)["idp-superset"]
    assert c["standardFlowEnabled"] and not c["directAccessGrantsEnabled"] and not c["publicClient"]
    claims = {m["config"].get("claim.name") for m in c["protocolMappers"]}
    assert {"tenant_id", "idp_principal", "groups"} <= claims
    assert all(u.startswith(("http://localhost:8088/", "http://superset.")) for u in c["redirectUris"])
    assert "bi-admins" in [g["name"] for g in r["groups"]]
    ada = next(u for u in r["users"] if u["username"] == "ada")
    assert ada["groups"] == ["/tenant_a", "/bi-admins"]
