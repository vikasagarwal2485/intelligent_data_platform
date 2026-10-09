# Serving API delta

## MODIFIED Requirements

### Requirement: REQ-SRV-03 — Tenant scoping and graceful model absence
When authentication is enabled, every API endpoint except `GET /health` SHALL
require a Bearer access token issued by the platform identity provider (Keycloak
realm). The token SHALL be validated for signature, issuer, audience and expiry.
The caller's tenant SHALL be the token's tenant claim, and the request SHALL be
served only within that tenant's scope:

- A missing, malformed, expired or otherwise invalid token SHALL receive HTTP 401.
- A valid token without a tenant claim SHALL receive HTTP 403.
- A tenant supplied in the request (the body's `tenant_id`, or the serving lookup's
  `X-Tenant-Id` header) is optional; one that differs from the token's tenant SHALL
  receive HTTP 403. Omitted, the token's tenant is used.

Authentication SHALL be enabled in every cluster deployment. It MAY be disabled only
for local development and tests, in which case the request's tenant (the body's
`tenant_id`, or the `X-Tenant-Id` header) is used as-is and is required. When models are not yet trained, the scoring endpoints SHALL return HTTP
503 rather than error.

#### Scenario: SCN-SRV-03.1 — Models not trained
- **WHEN** a scoring endpoint is called with a valid token before models exist in
  the artifact store
- **THEN** it responds with HTTP 503

#### Scenario: SCN-SRV-03.2 — Tenant echoed
- **WHEN** a scoring request with a valid `tenant_a` token succeeds
- **THEN** the response is scoped to `tenant_a`

#### Scenario: SCN-SRV-03.3 — Missing or invalid token
- **WHEN** `POST /credit-limit` is called with no token, an expired token, or a token
  signed by an unknown key
- **THEN** it responds with HTTP 401 and computes nothing

#### Scenario: SCN-SRV-03.4 — Cross-tenant request rejected
- **WHEN** a caller holding a `tenant_a` token calls
  `GET /serving/credit_scores/<service_id>` with `X-Tenant-Id: tenant_b`, or posts a
  body with `tenant_id = tenant_b`
- **THEN** it responds with HTTP 403 and returns no `tenant_b` data

#### Scenario: SCN-SRV-03.5 — Health stays open
- **WHEN** `GET /health` is called without a token
- **THEN** it returns `{"status": "ok"}`

#### Scenario: SCN-SRV-03.6 — Cluster deployment enforces auth
- **WHEN** the scoring API is deployed with either the local or aws overlay
- **THEN** authentication is enabled and an unauthenticated scoring call receives
  HTTP 401

### Requirement: REQ-SRV-04 — Precomputed serving lookup
The API SHALL expose `GET /serving/{table}/{service_id}`, which returns the
precomputed row for that subscriber of the caller's tenant from the TimescaleDB
serving copy. The tenant is the Bearer token's (REQ-SRV-03); an optional
`X-Tenant-Id` header is validated against it, and with authentication disabled the
header is required. The tenant is not part of the path. The endpoint SHALL NOT
recompute anything from features. `table` SHALL be one of
`credit_scores`, `fraud_scores`, `credit_features`, `fraud_features`.

#### Scenario: SCN-SRV-04.1 — Lookup a published score
- **WHEN** the serving store is enabled and published, and the endpoint is called for
  `credit_scores` and an existing service_id of the token's tenant, with or without
  a matching `X-Tenant-Id` header
- **THEN** it returns that row's columns

#### Scenario: SCN-SRV-04.2 — Serving store disabled
- **WHEN** the serving store is not enabled
- **THEN** the endpoint responds with HTTP 503

#### Scenario: SCN-SRV-04.3 — Unknown table or subscriber
- **WHEN** `table` is not one of the four serving tables, or no row matches
- **THEN** the endpoint responds with HTTP 404

#### Scenario: SCN-SRV-04.4 — Header without a token
- **WHEN** authentication is enabled and the lookup is called with `X-Tenant-Id` but
  no Bearer token
- **THEN** it responds with HTTP 401 and reads nothing
