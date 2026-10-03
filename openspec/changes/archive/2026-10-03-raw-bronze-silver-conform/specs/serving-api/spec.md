# Serving API delta

## MODIFIED Requirements

### Requirement: REQ-SRV-02 — Scoring endpoints
The system SHALL expose a FastAPI service with `GET /health`,
`POST /credit-limit`, and `POST /fraud-score`. Each scoring endpoint SHALL accept a
`tenant_id`, `service_id`, and a `features` map, and return the model outputs.
Features missing from the map SHALL default to `0`. A feature name that isn't in
the endpoint's model feature contract SHALL be rejected with HTTP 422 naming it, so
a misspelled or renamed feature (e.g. the former `account_tenure_days`) can't be
silently scored as `0`.

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

#### Scenario: SCN-SRV-02.4 — Unknown feature rejected
- **WHEN** `POST /credit-limit` is called with features including
  `account_tenure_days`, which isn't in the credit feature contract
- **THEN** it answers HTTP 422 naming `account_tenure_days`, and no score is computed

#### Scenario: SCN-SRV-02.5 — Missing feature defaults to zero
- **WHEN** `POST /fraud-score` is called with only some of the fraud features
- **THEN** the others are scored as `0`, as before
