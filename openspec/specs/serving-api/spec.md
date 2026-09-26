# Serving API Specification

## Purpose

Serve Gold to consumers: a low-latency serving store (TimescaleDB) materialized
from Gold, and an on-demand HTTP scoring API — both tenant-scoped.

## Requirements

### Requirement: REQ-SRV-01 — Gold serving materialization (TimescaleDB)
The system SHALL provide a `publish` stage that reads the Gold score and feature
tables (`credit_scores`, `fraud_scores`, `credit_features`, `fraud_features`) from
object storage and writes them to TimescaleDB `gold.*` tables indexed by
`(tenant_id, service_id)` for low-latency serving (ADR-0002). Each serving table
SHALL be replaced as a whole in a single transaction, so readers never see a partly
written table. Concurrent publishers of the same table SHALL be serialized rather
than fail. It SHALL be an independent stage (`idp publish`), run on demand or on a
schedule, and SHALL fail clearly when Gold has no tables to publish.

#### Scenario: SCN-SRV-01.1 — Publish Gold to TimescaleDB
- **WHEN** `idp publish` runs against a populated Gold layer
- **THEN** the Gold score/feature tables are written to TimescaleDB `gold.*`,
  indexed by `(tenant_id, service_id)`, and are queryable by that key

#### Scenario: SCN-SRV-01.2 — Independent of the API
- **WHEN** the publish stage runs
- **THEN** it performs no HTTP serving and does not require the scoring API

#### Scenario: SCN-SRV-01.3 — Concurrent publishers do not collide
- **WHEN** two publish runs replace the same serving table at the same time
- **THEN** both complete without a catalog/duplicate-type error, one after the other

#### Scenario: SCN-SRV-01.4 — Nothing to publish
- **WHEN** `idp publish` runs and Gold contains none of the serving tables
- **THEN** it fails with an error instructing to run the Gold stage first

### Requirement: REQ-SRV-02 — Scoring endpoints
The system SHALL expose a FastAPI service with `GET /health`,
`POST /credit-limit`, and `POST /fraud-score`. Each scoring endpoint SHALL accept a
`tenant_id`, `service_id`, and a `features` map, and return the model outputs.
Features missing from the map SHALL default to `0`.

#### Scenario: SCN-SRV-02.1 — Health
- **WHEN** `GET /health` is called
- **THEN** it returns `{"status": "ok"}`

#### Scenario: SCN-SRV-02.2 — Credit scoring
- **WHEN** `POST /credit-limit` is called with a tenant, subject, and features
- **THEN** the response contains `pd`, `affordability_base`, `recommended_limit`
  and `reason_codes` (a list of `{feature, contribution, method}` adverse reasons,
  at most 3) for that tenant

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

### Requirement: REQ-SRV-04 — Precomputed serving lookup
The API SHALL expose `GET /serving/{table}/{tenant_id}/{service_id}`, which returns
the precomputed row for that subscriber from the TimescaleDB serving copy. It
SHALL NOT recompute anything from features. `table` SHALL be one of
`credit_scores`, `fraud_scores`, `credit_features`, `fraud_features`.

#### Scenario: SCN-SRV-04.1 — Lookup a published score
- **WHEN** the serving store is enabled and published, and the endpoint is called for
  `credit_scores` with an existing tenant/service_id
- **THEN** it returns that row's columns

#### Scenario: SCN-SRV-04.2 — Serving store disabled
- **WHEN** the serving store is not enabled
- **THEN** the endpoint responds with HTTP 503

#### Scenario: SCN-SRV-04.3 — Unknown table or subscriber
- **WHEN** `table` is not one of the four serving tables, or no row matches
- **THEN** the endpoint responds with HTTP 404

### Requirement: REQ-SRV-05 — Serving holds the latest score per subscriber
When publishing score tables, the publish stage SHALL select for each tenant only
the rows of that tenant's latest `scored_at` run. Every serving table SHALL contain
at most one row per `(tenant_id, service_id)`, and the serving store SHALL enforce
this as a uniqueness constraint. The serving lookup (REQ-SRV-04) SHALL therefore
return a single, well-defined row: the latest score.

#### Scenario: SCN-SRV-05.1 — Latest run published
- **WHEN** Gold holds two score runs and `idp publish` runs
- **THEN** `gold.credit_scores` holds only the rows of the latest `scored_at`, one
  per `(tenant_id, service_id)`

#### Scenario: SCN-SRV-05.2 — Uniqueness enforced
- **WHEN** a serving table would receive two rows for the same
  `(tenant_id, service_id)`
- **THEN** the publish fails for that table and the previously published table stays
  intact, because the replace is transactional
