# Bronze Ingestion Specification

## Purpose

Capture raw records immutably and tenant-partitioned as the Bronze layer.

## Requirements

### Requirement: Immutable, tenant-partitioned Bronze
The system SHALL persist raw per-entity frames to the Bronze layer as Parquet
partitioned by `tenant_id`, to local dirs or `s3://<prefix>-bronze` per config,
without transformation.

#### Scenario: Write and reload Bronze
- **WHEN** `to_bronze(cfg, frames)` runs with tenant-stamped frames
- **THEN** each non-empty entity is written under `bronze/<entity>/` and can be
  read back with `tenant_id` restored

#### Scenario: Physical tenant partitioning
- **WHEN** a multi-tenant entity is written to Bronze
- **THEN** the on-disk/object layout contains a partition per `tenant_id`

### Requirement: Bronze is a standalone stage
The Bronze stage SHALL NOT perform Silver or Gold work; it only lands raw data.

#### Scenario: No chaining
- **WHEN** the Bronze stage completes
- **THEN** no Silver or Gold tables have been produced by it
