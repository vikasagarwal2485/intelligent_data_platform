# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-SRV-03 — Tenant scoping and graceful model absence
When authentication is enabled, every API endpoint except `GET /health` SHALL
require a Bearer access token issued by the platform identity provider (Keycloak
realm). The token SHALL be validated for signature, issuer, audience and expiry.
The caller's tenant SHALL be the token's tenant claim, and the request SHALL be
served only within that tenant's scope:

- A missing, malformed, expired or otherwise invalid token SHALL receive HTTP 401.
- A valid token without a tenant claim SHALL receive HTTP 403.
- A `tenant_id` supplied in the request body or path that differs from the token's
  tenant SHALL receive HTTP 403.

Authentication SHALL be enabled in every cluster deployment. It MAY be disabled only
for local development and tests, in which case the request's `tenant_id` field is
used as-is. When models are not yet trained, the scoring endpoints SHALL return HTTP
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
  `GET /serving/credit_scores/tenant_b/<service_id>`, or posts a body with
  `tenant_id = tenant_b`
- **THEN** it responds with HTTP 403 and returns no `tenant_b` data

#### Scenario: SCN-SRV-03.5 — Health stays open
- **WHEN** `GET /health` is called without a token
- **THEN** it returns `{"status": "ok"}`

#### Scenario: SCN-SRV-03.6 — Cluster deployment enforces auth
- **WHEN** the scoring API is deployed with either the local or aws overlay
- **THEN** authentication is enabled and an unauthenticated scoring call receives
  HTTP 401
