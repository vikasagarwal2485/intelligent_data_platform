# Spec Delta

## ADDED Requirements

### Requirement: REQ-SLV-06 — Idempotent Silver writes
Persisting Silver SHALL replace, for each table, the partitions of the tenants
present in the new output. Re-running the Silver stage on unchanged Bronze SHALL
leave every Silver table with identical contents. Partitions of tenants absent from
the new output SHALL be left untouched.

#### Scenario: SCN-SLV-06.1 — Re-run does not duplicate
- **WHEN** the Silver stage runs twice on the same Bronze and persists both times
- **THEN** each Silver table has the same row count after the second run as after
  the first, and `dim_subscriber` still has one row per `(tenant_id, service_id)`

#### Scenario: SCN-SLV-06.2 — Other tenants untouched
- **WHEN** Silver is persisted with output for `tenant_a` only, while `tenant_b`
  partitions already exist
- **THEN** the `tenant_a` partitions are replaced and the `tenant_b` partitions are
  unchanged
