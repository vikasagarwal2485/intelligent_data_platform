# Spec Delta

## ADDED Requirements

### Requirement: REQ-FND-05 — Analytics consent enforced on the ML path
The Gold feature tables SHALL contain only subscribers whose `consent_analytics` is
true. Because model training, the Score stage, the serving store and the score
analytics all read Gold, a non-consenting subscriber SHALL never be trained on,
scored, published or reported per subscriber. A null or missing consent value SHALL
be treated as no consent. If the subscriber data has no consent column at all, the
Gold stage SHALL fail with a clear error rather than include every subscriber.

#### Scenario: SCN-FND-05.1 — Non-consenting subscriber excluded from features
- **WHEN** Gold is built from Silver in which subscriber S has
  `consent_analytics = false`
- **THEN** S is absent from `credit_features` and `fraud_features`, and every
  consenting subscriber is present exactly once

#### Scenario: SCN-FND-05.2 — Excluded downstream
- **WHEN** training, Score and publish run after that Gold build
- **THEN** S appears in no training set, in neither score table, and in no
  `gold.*` serving table

#### Scenario: SCN-FND-05.3 — Null consent fails closed
- **WHEN** subscriber S has a null `consent_analytics`
- **THEN** S is excluded exactly as if consent were false

#### Scenario: SCN-FND-05.4 — Missing consent column is an error
- **WHEN** the Gold stage runs on a `dim_subscriber` that has no
  `consent_analytics` column
- **THEN** it fails with an error naming the missing consent column and writes no
  Gold tables

#### Scenario: SCN-FND-05.5 — Revocation takes effect on refresh
- **WHEN** S revokes consent, then Gold is refreshed and Score and publish run
- **THEN** S no longer appears in the Gold feature tables, the latest score run, or
  the serving tables

### Requirement: REQ-FND-06 — Tenant-scoped reads go through the gateway
Every tenant-scoped read that serves a caller's request SHALL obtain its rows
through the tenant gateway (`scope_to_tenant`). This covers the MCP analytics
tools; the serving lookup filters by tenant in its query. Data lacking the
`tenant_id` column SHALL be rejected as a tenant-isolation violation. It SHALL NOT be
treated as empty, and it SHALL NOT be returned unscoped.

#### Scenario: SCN-FND-06.1 — Untagged table is an isolation error
- **WHEN** an MCP tool reads a lake table that has no `tenant_id` column
- **THEN** the tool returns a structured error identifying a tenant-isolation
  violation and the table, and returns no rows

#### Scenario: SCN-FND-06.2 — Gateway scopes every tool
- **WHEN** any MCP tool is called for `tenant_a` on a lake holding `tenant_a` and
  `tenant_b`
- **THEN** the rows it aggregates are exactly those that `scope_to_tenant` returns
  for `tenant_a`
