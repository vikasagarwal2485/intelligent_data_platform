"""The MCP server's identity (REQ-MCP-06, REQ-TEN-01).

The server authenticates with its tenant's Keycloak MCP client (client credentials,
``idp-mcp-<tenant>``). The access token it gets carries ``idp_principal =
mcp@<tenant>`` and ``tenant_id``; Trino verifies the token on every query and
row-filters to that tenant (REQ-TEN-03), and the tools answer only for that tenant.

:class:`ClientCredentialsProvider` caches the token until ``refresh_margin_seconds``
before it expires, then fetches a new one. Its claims are read without verifying
the signature: the token comes straight from the identity provider, and the query
engine verifies it. Tests install their own provider with :func:`use_provider`.
"""
from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol

from idp.common.config import PlatformConfig


class IdentityError(Exception):
    """No token could be obtained: the identity provider is unreachable or refused
    the client credentials. ``source`` is the token endpoint."""

    def __init__(self, message: str, source: str = "") -> None:
        super().__init__(message)
        self.source = source


class TokenProvider(Protocol):
    source: str

    def token(self) -> str: ...

    @property
    def tenant(self) -> str: ...


def claims(token: str) -> dict[str, Any]:
    """A JWT's payload (unverified)."""
    import jwt
    return jwt.decode(token, options={"verify_signature": False})


class ClientCredentialsProvider:
    """Access tokens for one client, renewed shortly before they expire."""

    def __init__(self, token_url: str, client_id: str, client_secret: str,
                 refresh_margin_seconds: int = 30, timeout: float = 10.0, clock=time.time) -> None:
        self.source = token_url
        self._form = urllib.parse.urlencode({"grant_type": "client_credentials",
                                             "client_id": client_id,
                                             "client_secret": client_secret}).encode()
        self._margin = refresh_margin_seconds
        self._timeout = timeout
        self._clock = clock
        self._lock = threading.Lock()
        self._token: str | None = None
        self._claims: dict[str, Any] = {}

    def _fetch(self) -> str:
        req = urllib.request.Request(self.source, data=self._form, method="POST",
                                     headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:      # noqa: S310 - configured endpoint
                body = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:200]
            raise IdentityError(f"the identity provider refused the MCP client credentials "
                                f"(HTTP {e.code}): {detail}", self.source) from e
        except (urllib.error.URLError, OSError, ValueError) as e:
            raise IdentityError(f"could not reach the identity provider at {self.source}: "
                                f"{type(e).__name__}: {e}", self.source) from e
        token = body.get("access_token")
        if not token:
            raise IdentityError(f"the identity provider returned no access token: {str(body)[:200]}",
                                self.source)
        return token

    def token(self) -> str:
        with self._lock:
            exp = self._claims.get("exp", 0)
            if self._token is None or self._clock() >= exp - self._margin:
                token = self._fetch()
                c = claims(token)
                if not c.get("tenant_id"):
                    raise IdentityError("the MCP client's token carries no tenant_id claim", self.source)
                self._token, self._claims = token, c
            return self._token

    @property
    def tenant(self) -> str:
        if self._token is None:
            self.token()
        return str(self._claims["tenant_id"])


_override: TokenProvider | None = None
_cache: dict[tuple, ClientCredentialsProvider] = {}
_cache_lock = threading.Lock()


def use_provider(provider: TokenProvider | None) -> None:
    """Install a provider for every query (tests); ``None`` restores the configured one."""
    global _override
    _override = provider


def provider(cfg: PlatformConfig) -> TokenProvider | None:
    """The configured identity, or ``None`` when MCP authentication is disabled
    (tests with a fake query engine only)."""
    if _override is not None:
        return _override
    a = cfg.mcp["auth"]
    if not a["enabled"]:
        return None
    if not a["token_url"] or not a["client_id"]:
        raise IdentityError("MCP authentication is enabled but IDP_MCP_TOKEN_URL or "
                            "IDP_MCP_CLIENT_ID is not set", a["token_url"])
    key = (a["token_url"], a["client_id"], a["client_secret"], a["refresh_margin_seconds"])
    with _cache_lock:
        if key not in _cache:
            _cache[key] = ClientCredentialsProvider(a["token_url"], a["client_id"], a["client_secret"],
                                                    a["refresh_margin_seconds"])
        return _cache[key]
