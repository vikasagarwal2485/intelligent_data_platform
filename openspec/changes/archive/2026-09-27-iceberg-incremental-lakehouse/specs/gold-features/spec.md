# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-GLD-04 — Idempotent Gold feature writes
Persisting `credit_features` and `fraud_features` SHALL atomically replace the
partitions of the tenants present in the new output. Each feature table SHALL then
contain exactly one row per `(tenant_id, service_id)`, however many times the Gold
stage has run. Partitions of other tenants SHALL be left untouched. Gold SHALL be
recomputed in full from Silver on every run.

#### Scenario: SCN-GLD-04.1 — Hourly refresh keeps one row per subscriber
- **WHEN** the Gold stage runs repeatedly against the same lake (as the hourly
  features DAG does)
- **THEN** reading `credit_features` back yields exactly one row per
  `(tenant_id, service_id)`

#### Scenario: SCN-GLD-04.2 — Refresh reflects new Silver
- **WHEN** Silver changes between two Gold runs
- **THEN** after the second run the Gold feature values reflect only the new Silver,
  with no rows left over from the first run
