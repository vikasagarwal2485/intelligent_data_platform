# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-BRZ-01 — Immutable, tenant-partitioned Bronze
The system SHALL land raw per-entity records into Bronze as Iceberg tables
(`bronze.<entity>`), without transformation, partitioned by `tenant_id` and
`ingest_date`. Each landed record SHALL carry the time it was landed
(`_ingested_at`, UTC) and the derived `ingest_date`. Bronze SHALL be append-only:
landing never rewrites or deletes earlier records, so Bronze stays a complete raw
history that can be reprocessed.

#### Scenario: SCN-BRZ-01.1 — Write and reload Bronze
- **WHEN** `to_bronze(cfg, frames)` runs with tenant-stamped frames
- **THEN** each non-empty entity is appended to the Iceberg table `bronze.<entity>`,
  and reading it back returns the landed records with `tenant_id`, `_ingested_at`
  and `ingest_date`

#### Scenario: SCN-BRZ-01.2 — Physical tenant partitioning
- **WHEN** a multi-tenant entity is written to Bronze
- **THEN** the table's partition spec includes `tenant_id` and `ingest_date`, so a
  read filtered to one tenant and day touches only that partition's files

#### Scenario: SCN-BRZ-01.3 — Append-only history
- **WHEN** the same entity is landed in two batches, including records that repeat
  earlier ones
- **THEN** Bronze holds both batches as separate snapshots, and no earlier record is
  modified or removed
