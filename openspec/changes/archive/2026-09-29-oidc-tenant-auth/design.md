# Design

## Context

- `serving/api.py` builds the app in `create_app()`; `ScoreRequest` carries
  `tenant_id`, and `/serving/{table}/{tenant_id}/{service_id}` takes the tenant from
  the path.
- Keycloak 26 runs `start-dev` (H2, in memory) in `idp-platform` on port 8080, with
  no realm configured. Because the store is in memory, any configuration must be
  re-applied on every pod start.
- Tests build the app with `create_app(artifacts_dir=…)` and call it through
  `TestClient`.

## Goals / Non-Goals

**Goals:**
- Stateless JWT validation in the API, with the tenant from a claim.
- A reproducible realm with dev users per tenant, so the flow works on minikube.
- Offline tests with no Keycloak needed.

**Non-Goals:**
- Authenticating the MCP server (local stdio, operator-run).
- Role-based authorization beyond tenant membership (e.g. read-only vs scoring
  roles).
- Service-to-service auth for batch stages, which never call the API.
- A production Keycloak (Postgres-backed, HA) for EKS. This change delivers the
  config shape; the hardening is separate.

## Decisions

- **PyJWT with `PyJWKClient`** against `{issuer}/protocol/openid-connect/certs`.
  Keys are cached, with a refetch on unknown `kid`. Validation covers the `RS256`
  signature, `iss == auth.issuer`, `aud` contains `auth.audience`, and
  `exp`/`nbf` with 30 s leeway.
  - Alternative: token introspection per request. Rejected because it adds latency
    and makes Keycloak a hard runtime dependency on every call.
- **FastAPI dependency `current_tenant()`** in the new `serving/auth.py`:
  - returns the tenant from the claim (`auth.tenant_claim`, default `tenant_id`)
  - raises 401 on a missing or invalid token, 403 on a missing claim
  - when `auth.enabled` is false, returns `None`, and the handlers use the
    request's `tenant_id` (current behaviour)
- **Mismatch check in the handlers**: when the token's tenant is set and the
  body/path `tenant_id` differs, return 403. The body's `tenant_id` stays in the
  schema for backwards compatibility and dev mode, but is always overridden by the
  token's tenant when auth is on.
- **Config**: an `auth` section (`enabled`, `issuer`, `audience`, `tenant_claim`,
  `leeway_seconds`) with `IDP_AUTH_ENABLED`, `IDP_AUTH_ISSUER` and
  `IDP_AUTH_AUDIENCE` overrides. The default is `enabled: false` in `platform.yaml`;
  both app overlays set `IDP_AUTH_ENABLED=true`.
- **Keycloak realm import**: a ConfigMap holding `idp-realm.json`, mounted at
  `/opt/keycloak/data/import`, with the container args changed to
  `["start-dev", "--import-realm"]`. The realm contains:
  - the `idp-scoring-api` client (confidential, service accounts enabled, audience
    mapper)
  - a user-attribute mapper that puts `tenant_id` into access tokens
  - dev users `alice@tenant_a` and `bob@tenant_b` with `tenant_id` attributes
  - dev clients per tenant using client-credentials, whose hard-coded `tenant_id`
    claim mappers serve machine callers

  Because the store is in memory, the import re-applies on every restart.
- **Issuer URL.** In the cluster, tokens are minted with issuer
  `http://keycloak.idp-platform.svc:8080/realms/idp`. Clients must fetch tokens
  from the same hostname (via `KC_HOSTNAME`) so that `iss` matches, including
  through port-forward (`make keycloak-token` wraps this).
- **Tests** generate an RSA keypair, serve its JWKS through a patched
  `PyJWKClient`, and mint tokens covering valid, expired, wrong-key, no-claim and
  cross-tenant cases.

## Risks / Trade-offs

- [Issuer mismatch between port-forwarded and in-cluster token requests] → Pin
  `KC_HOSTNAME` to the service DNS name, and have `make keycloak-token` request
  tokens with that Host header.
- [Keycloak unavailable at API start or JWKS fetch time] → Fetch the JWKS lazily
  and cache it. If it cannot be fetched, requests get 503 (auth backend
  unavailable), never an unauthenticated pass-through.
- [Breaking change for existing in-cluster callers or scripts] → Document the token
  flow in `deploy/README.md`, and provide a `make keycloak-token TENANT=tenant_a`
  helper.
- [Dev realm secrets in git] → Dev-only values, clearly labelled; the aws overlay
  expects secrets from a Secret/IRSA.

## Migration Plan

1. `kubectl apply` the realm ConfigMap and the updated Keycloak Deployment. Verify
   that the realm `idp` exists.
2. `make image && make app-deploy` (overlays now set `IDP_AUTH_ENABLED=true`).
3. Verify: an unauthenticated scoring call gets 401, and `make keycloak-token` plus
   a Bearer call gets 200.
4. Rollback: set `IDP_AUTH_ENABLED=false` on the Deployment. This is dev only and
   must not be done on EKS.
