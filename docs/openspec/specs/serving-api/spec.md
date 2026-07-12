# Serving API Specification

## Purpose

Serve Gold to consumers: a low-latency serving store (TimescaleDB) materialized
from Gold, and an on-demand HTTP scoring API — both tenant-scoped.

## Requirements

### Requirement: REQ-SRV-01 — Gold serving materialization (TimescaleDB)
The system SHALL provide a `publish` stage that reads Gold from object storage and
writes it to TimescaleDB `gold.*` tables indexed by `(tenant_id, service_id)` for
low-latency serving (ADR-0002). It SHALL be an independent stage (`idp publish`)
run on demand or on a schedule.

#### Scenario: SCN-SRV-01.1 — Publish Gold to TimescaleDB
- **WHEN** `idp publish` runs against a populated Gold layer
- **THEN** the Gold score/feature tables are upserted into TimescaleDB `gold.*`
  keyed by `(tenant_id, service_id)` and are queryable by that key

#### Scenario: SCN-SRV-01.2 — Independent of the API
- **WHEN** the publish stage runs
- **THEN** it performs no HTTP serving and does not require the scoring API

### Requirement: REQ-SRV-02 — Scoring endpoints
The system SHALL expose a FastAPI service with `GET /health`,
`POST /credit-limit`, and `POST /fraud-score`, each accepting a `tenant_id`,
`service_id`, and a `features` map, and returning the model outputs.

#### Scenario: SCN-SRV-02.1 — Health
- **WHEN** `GET /health` is called
- **THEN** it returns `{"status": "ok"}`

#### Scenario: SCN-SRV-02.2 — Credit scoring
- **WHEN** `POST /credit-limit` is called with a tenant, subject, and features
- **THEN** the response contains `pd`, `affordability_base`, and
  `recommended_limit` for that tenant

#### Scenario: SCN-SRV-02.3 — Fraud scoring
- **WHEN** `POST /fraud-score` is called
- **THEN** the response contains `fraud_proba`, `anomaly_score`, `rule_score`,
  and `fraud_score`

### Requirement: REQ-SRV-03 — Tenant scoping and graceful model absence
Every request SHALL carry a `tenant_id` (from the validated OIDC token in
production) and be served only within that tenant's scope; when models are not
yet trained the endpoints SHALL return HTTP 503 rather than error.

#### Scenario: SCN-SRV-03.1 — Models not trained
- **WHEN** a scoring endpoint is called before models exist in the artifact store
- **THEN** it responds with HTTP 503

#### Scenario: SCN-SRV-03.2 — Tenant echoed
- **WHEN** a scoring request for `tenant_a` succeeds
- **THEN** the response is scoped to `tenant_a`
