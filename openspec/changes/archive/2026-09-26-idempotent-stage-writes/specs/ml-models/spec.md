# Spec Delta

## ADDED Requirements

### Requirement: REQ-ML-08 — Idempotent labels and per-run score snapshots
Re-seeding the labels store SHALL replace the labels of the tenants being seeded;
it SHALL NOT append a second copy. Score tables are append-only history (REQ-ML-07).
Each Score run SHALL contribute exactly one row per `(tenant_id, service_id)`, all
stamped with that run's `scored_at`, so any single run can be read back as a
complete, duplicate-free snapshot.

#### Scenario: SCN-ML-08.1 — Re-seeding labels does not stack
- **WHEN** `idp labels` runs twice with the same parameters
- **THEN** the labels store holds exactly one label per `(tenant_id, service_id)`,
  and it is the label from the latest seeding

#### Scenario: SCN-ML-08.2 — One row per subscriber per score run
- **WHEN** the Score stage runs twice
- **THEN** `credit_scores` holds two runs, and filtering on either `scored_at`
  yields exactly one row per `(tenant_id, service_id)`
