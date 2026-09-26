# Proposal

## Why

REQ-SRV-03 says every request's tenant comes "from the validated OIDC token in
production". The scoring API has no authentication at all: `tenant_id` is a plain
request-body or path field, so any caller can name any tenant and read that
tenant's precomputed scores through `/serving/...`. Keycloak is deployed in
`idp-platform` but unconfigured: `start-dev`, no realm, no client. Tenant isolation
at the edge is therefore only nominal.

## What Changes

- The scoring API validates a Bearer JWT from the platform Keycloak realm on every
  endpoint except `GET /health`: signature via JWKS, issuer, audience and expiry.
- The caller's tenant is taken from the token's tenant claim. A `tenant_id` given in
  the body or path must match it (403 otherwise). A missing or invalid token gets a
  401.
- Authentication can be disabled only for local dev and tests (`auth.enabled=false`,
  the default in `platform.yaml`). The cluster overlays enable it.
- Keycloak gets a realm (`idp`), an API client (`idp-scoring-api`), a `tenant_id`
  token mapper and dev users per tenant, imported at startup.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `serving-api`: REQ-SRV-03 becomes token-derived tenant scoping with 401/403
  semantics (the 503-when-untrained behaviour is kept).

## Impact

- Code: `src/idp/serving/api.py` (auth dependency on all scoring/lookup routes);
  new `src/idp/serving/auth.py`; `src/idp/common/config.py` (`auth` section,
  `IDP_AUTH_*` env).
- Dependencies: `PyJWT[crypto]`.
- Deploy: a Keycloak realm-import ConfigMap and `--import-realm`; `IDP_AUTH_*` env
  on the scoring-api Deployment in both overlays.
- **BREAKING** for API clients in the cluster: they must send a Bearer token.
  Local `make serve` is unchanged (auth disabled by default).
- Depends on `sync-specs-with-implementation`, which adds the `/serving/...` lookup
  (REQ-SRV-04) that this change protects.
