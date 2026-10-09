"""Bearer-token authentication and tenant scoping for the scoring API (REQ-SRV-03).

When ``auth.enabled`` is on, every endpoint except ``GET /health`` needs an access
token from the platform Keycloak realm. :class:`TokenVerifier` validates it
statelessly against the realm's JWKS (``{issuer}/protocol/openid-connect/certs``,
fetched lazily and cached, refetched on an unknown ``kid``): the ``RS256``
signature, ``iss``, ``aud`` and ``exp``/``nbf`` with a small leeway. The caller's
tenant is the token's tenant claim.

* A missing, malformed or invalid token gets 401; a valid token without a tenant
  claim gets 403.
* If the JWKS can't be fetched, the request gets 503 (auth backend unavailable),
  never an unauthenticated pass-through.
* With auth off (local dev and tests) the dependency returns ``None`` and the
  handlers use the request's ``tenant_id`` as before.
"""
from __future__ import annotations

from typing import Any, Callable

import jwt
from fastapi import HTTPException, Request
from jwt import PyJWKClient

_ALGORITHMS = ["RS256"]


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(401, detail, headers={"WWW-Authenticate": "Bearer"})


class TokenVerifier:
    """Validates Keycloak access tokens and returns the caller's tenant."""

    def __init__(self, auth: dict[str, Any]):
        self.issuer = str(auth["issuer"]).rstrip("/")
        self.audience = str(auth["audience"])
        self.tenant_claim = str(auth.get("tenant_claim", "tenant_id"))
        self.leeway = int(auth.get("leeway_seconds", 30))
        if not self.issuer:
            raise ValueError("auth.enabled needs auth.issuer (IDP_AUTH_ISSUER)")
        self._jwks: PyJWKClient | None = None

    @property
    def jwks_uri(self) -> str:
        return f"{self.issuer}/protocol/openid-connect/certs"

    def _client(self) -> PyJWKClient:
        if self._jwks is None:                       # lazily: Keycloak may start after the API
            self._jwks = PyJWKClient(self.jwks_uri, cache_keys=True, lifespan=300, timeout=5)
        return self._jwks

    def tenant(self, token: str) -> str:
        """The tenant of a valid token. Raises 401/403/503 as ``HTTPException``."""
        try:
            key = self._client().get_signing_key_from_jwt(token)
        except jwt.PyJWKClientConnectionError as e:
            raise HTTPException(503, f"auth backend unavailable: {e}") from e
        except (jwt.PyJWKClientError, jwt.DecodeError) as e:
            raise _unauthorized(f"invalid token: {e}") from e
        try:
            claims = jwt.decode(token, key.key, algorithms=_ALGORITHMS, audience=self.audience,
                                issuer=self.issuer, leeway=self.leeway,
                                options={"require": ["exp", "iss", "aud"]})
        except jwt.InvalidTokenError as e:
            raise _unauthorized(f"invalid token: {e}") from e
        tenant = claims.get(self.tenant_claim)
        if not tenant or not isinstance(tenant, str):
            raise HTTPException(403, f"token has no {self.tenant_claim!r} claim")
        return tenant


def tenant_dependency(auth: dict[str, Any]) -> Callable[[Request], str | None]:
    """A FastAPI dependency giving the caller's tenant from the Bearer token, or
    ``None`` when auth is disabled."""
    if not auth.get("enabled"):
        def no_auth(request: Request) -> str | None:
            return None
        return no_auth
    verifier = TokenVerifier(auth)

    def current_tenant(request: Request) -> str | None:
        header = request.headers.get("authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            raise _unauthorized("missing Bearer token")
        return verifier.tenant(token.strip())

    current_tenant.verifier = verifier               # type: ignore[attr-defined]
    return current_tenant


def scoped_tenant(requested: str | None, token_tenant: str | None, field: str = "tenant_id") -> str:
    """The tenant a request is served for: the token's, when auth is on (a
    different tenant in the request is a 403, an omitted one means the token's);
    else the request's, which is then required. ``field`` names where the request
    carries it (a body field, or the ``X-Tenant-Id`` header) in the error."""
    if token_tenant is None:
        if not requested:
            raise HTTPException(422, f"{field} is required")
        return requested
    if requested and requested != token_tenant:
        raise HTTPException(403, f"{field} does not match the token's tenant")
    return token_tenant
