# Serving API Specification

## Purpose

Expose on-demand credit-limit and fraud scoring over HTTP, tenant-scoped.

## Requirements

### Requirement: Scoring endpoints
The system SHALL expose a FastAPI service with `GET /health`,
`POST /credit-limit`, and `POST /fraud-score`, each accepting a `tenant_id`,
`service_id`, and a `features` map, and returning the model outputs.

#### Scenario: Health
- **WHEN** `GET /health` is called
- **THEN** it returns `{"status": "ok"}`

#### Scenario: Credit scoring
- **WHEN** `POST /credit-limit` is called with a tenant, subject, and features
- **THEN** the response contains `pd`, `affordability_base`, and
  `recommended_limit` for that tenant

#### Scenario: Fraud scoring
- **WHEN** `POST /fraud-score` is called
- **THEN** the response contains `fraud_proba`, `anomaly_score`, `rule_score`,
  and `fraud_score`

### Requirement: Tenant scoping and graceful model absence
Every request SHALL carry a `tenant_id` (from the validated OIDC token in
production) and be served only within that tenant's scope; when models are not
yet trained the endpoints SHALL return HTTP 503 rather than error.

#### Scenario: Models not trained
- **WHEN** a scoring endpoint is called before models exist in the artifact store
- **THEN** it responds with HTTP 503

#### Scenario: Tenant echoed
- **WHEN** a scoring request for `tenant_a` succeeds
- **THEN** the response is scoped to `tenant_a`
