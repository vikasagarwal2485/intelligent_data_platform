"""The MCP server's identity: client-credentials tokens (REQ-MCP-06, REQ-TEN-01)."""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

import jwt
import pytest

from idp.common.config import load_config
from idp.mcp import auth


class _TokenEndpoint:
    """A local token endpoint: client-credentials grants for one client, each token
    valid ``lifetime`` seconds from ``now()``; counts the requests."""

    def __init__(self, tenant="tenant_a", secret="s3cret", lifetime=300, status=200, now=time.time):
        self.requests, self.tenant, self.secret = 0, tenant, secret
        self.lifetime, self.status, self.now = lifetime, status, now
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                outer.requests += 1
                form = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
                ok = form.get("client_secret") == [outer.secret] and outer.status == 200
                claims = {"exp": int(outer.now()) + outer.lifetime, "aud": "trino",
                          "idp_principal": f"mcp@{outer.tenant}"}
                if outer.tenant:
                    claims["tenant_id"] = outer.tenant
                body = ({"access_token": jwt.encode(claims, "k" * 32, algorithm="HS256")} if ok
                        else {"error": "unauthorized_client"})
                self.send_response(200 if ok else (outer.status if outer.status != 200 else 401))
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())

            def log_message(self, *a):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/realms/idp/protocol/openid-connect/token"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()


@pytest.fixture()
def endpoint():
    e = _TokenEndpoint()
    yield e
    e.close()


def test_token_carries_the_tenant_and_is_reused(endpoint):
    p = auth.ClientCredentialsProvider(endpoint.url, "idp-mcp-tenant_a", "s3cret")
    t1, t2 = p.token(), p.token()
    assert t1 == t2 and endpoint.requests == 1
    assert p.tenant == "tenant_a" and auth.claims(t1)["idp_principal"] == "mcp@tenant_a"


def test_expiring_token_is_renewed():
    # SCN-MCP-06.7: within the refresh margin of expiry, the next call fetches anew
    clock = [1_000_000.0]
    e = _TokenEndpoint(lifetime=300, now=lambda: clock[0])
    try:
        p = auth.ClientCredentialsProvider(e.url, "c", "s3cret", refresh_margin_seconds=30,
                                           clock=lambda: clock[0])
        p.token()
        clock[0] += 260                       # 40 s left: still valid
        p.token()
        assert e.requests == 1
        clock[0] += 15                        # 25 s left: inside the margin
        p.token()
        assert e.requests == 2
    finally:
        e.close()


def test_rejected_credentials_are_an_identity_error(endpoint):
    # SCN-MCP-06.4: the identity provider's refusal names it as the source
    p = auth.ClientCredentialsProvider(endpoint.url, "idp-mcp-tenant_a", "wrong")
    with pytest.raises(auth.IdentityError, match="refused") as e:
        p.token()
    assert e.value.source == endpoint.url


def test_unreachable_identity_provider_is_an_identity_error():
    url = "http://127.0.0.1:9/realms/idp/protocol/openid-connect/token"   # nothing listens
    with pytest.raises(auth.IdentityError, match="could not reach") as e:
        auth.ClientCredentialsProvider(url, "c", "s", timeout=2).token()
    assert e.value.source == url


def test_token_without_a_tenant_is_refused():
    e = _TokenEndpoint(tenant="")
    try:
        with pytest.raises(auth.IdentityError, match="tenant_id"):
            auth.ClientCredentialsProvider(e.url, "c", "s3cret").token()
    finally:
        e.close()


def test_configured_provider_and_override(monkeypatch, endpoint):
    for var in ("IDP_MCP_TOKEN_URL", "IDP_MCP_CLIENT_ID", "IDP_MCP_CLIENT_SECRET"):
        monkeypatch.delenv(var, raising=False)
    assert load_config().mcp["auth"]["enabled"] is False
    assert auth.provider(load_config()) is None                       # disabled: no identity
    monkeypatch.setenv("IDP_MCP_TOKEN_URL", endpoint.url)
    monkeypatch.setenv("IDP_MCP_CLIENT_ID", "idp-mcp-tenant_a")
    monkeypatch.setenv("IDP_MCP_CLIENT_SECRET", "s3cret")
    cfg = load_config()
    assert cfg.mcp["auth"]["enabled"] is True
    p = auth.provider(cfg)
    assert p is auth.provider(cfg) and p.tenant == "tenant_a"         # cached per configuration
    fake = type("P", (), {"source": "x", "tenant": "tenant_b", "token": lambda self: "t"})()
    auth.use_provider(fake)
    try:
        assert auth.provider(cfg) is fake
    finally:
        auth.use_provider(None)


def test_enabled_without_endpoint_is_an_identity_error(monkeypatch):
    monkeypatch.delenv("IDP_MCP_TOKEN_URL", raising=False)
    cfg = load_config()
    cfg.raw["mcp"] = {"auth": {"enabled": True, "token_url": "", "client_id": ""}}
    with pytest.raises(auth.IdentityError, match="IDP_MCP_CLIENT_ID"):
        auth.provider(cfg)
