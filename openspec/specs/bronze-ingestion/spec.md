# Bronze Ingestion Specification

## Purpose

Capture raw records immutably and tenant-partitioned as the Bronze layer.

## Requirements

### Requirement: REQ-BRZ-01 — Immutable, tenant-partitioned Bronze
The system SHALL persist raw per-entity frames to the Bronze layer as Parquet
partitioned by `tenant_id`, to local dirs or `s3://<prefix>-bronze` per config,
without transformation.

#### Scenario: SCN-BRZ-01.1 — Write and reload Bronze
- **WHEN** `to_bronze(cfg, frames)` runs with tenant-stamped frames
- **THEN** each non-empty entity is written under `bronze/<entity>/` and can be
  read back with `tenant_id` restored

#### Scenario: SCN-BRZ-01.2 — Physical tenant partitioning
- **WHEN** a multi-tenant entity is written to Bronze
- **THEN** the on-disk/object layout contains a partition per `tenant_id`

### Requirement: REQ-BRZ-02 — Bronze is a standalone stage
The Bronze stage SHALL NOT perform Silver or Gold work; it only lands raw data.

#### Scenario: SCN-BRZ-02.1 — No chaining
- **WHEN** the Bronze stage completes
- **THEN** no Silver or Gold tables have been produced by it

### Requirement: REQ-BRZ-03 — File landing zone
The system SHALL provide a file landing zone (`idp generate`) that writes each
generated entity as a JSON array of records to `<landing>/tenant=<tenant_id>/<entity>.json`,
one file per tenant and entity. This simulates source pickup. It is independent of
the Kafka path and does not write Bronze.

#### Scenario: SCN-BRZ-03.1 — Per-tenant landing files
- **WHEN** `idp generate` runs for tenants `tenant_a` and `tenant_b`
- **THEN** `tenant=tenant_a/` and `tenant=tenant_b/` each contain one `<entity>.json`
  per non-empty entity, and every record carries its own `tenant_id`

#### Scenario: SCN-BRZ-03.2 — Landing does not populate Bronze
- **WHEN** `idp generate` completes
- **THEN** no Bronze tables have been written by it
