# Spec Delta

## ADDED Requirements

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
