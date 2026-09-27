# Bronze Ingestion Specification

## Purpose

Capture raw records immutably and tenant-partitioned as the Bronze layer.

## Requirements

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

### Requirement: REQ-BRZ-02 — Bronze is a standalone stage
The Bronze stage SHALL NOT perform Silver or Gold work; it only lands raw data.

#### Scenario: SCN-BRZ-02.1 — No chaining
- **WHEN** the Bronze stage completes
- **THEN** no Silver or Gold tables have been produced by it

### Requirement: REQ-BRZ-03 — File landing zone
The system SHALL provide a file landing zone (`idp generate`) that writes each
generated entity as a JSON array of records to `<landing>/tenant=<tenant_id>/<entity>.json`,
one file per tenant and entity. This simulates a source system's file drop.

The landing location SHALL be the local landing directory, or the bucket
`<prefix>-landing` when S3 is enabled. `idp generate` SHALL generate and write one
tenant at a time, to bound memory. The landing zone SHALL be the source of the
Kafka path when the producer publishes from it (REQ-STR-01). Writing the landing
zone SHALL NOT write Bronze.

#### Scenario: SCN-BRZ-03.1 — Per-tenant landing files
- **WHEN** `idp generate` runs for tenants `tenant_a` and `tenant_b`
- **THEN** `tenant=tenant_a/` and `tenant=tenant_b/` each contain one `<entity>.json`
  per non-empty entity, and every record carries its own `tenant_id`

#### Scenario: SCN-BRZ-03.2 — Landing does not populate Bronze
- **WHEN** `idp generate` completes
- **THEN** no Bronze tables have been written by it

#### Scenario: SCN-BRZ-03.3 — Landing on object storage
- **WHEN** S3 is enabled with prefix `idp` and `idp generate` runs
- **THEN** the landing files are written under `s3://idp-landing/tenant=<tenant_id>/`,
  with the same names and contents as a local run

#### Scenario: SCN-BRZ-03.4 — Landing feeds the Kafka path
- **WHEN** `idp generate` runs, then the producer publishes from the landing zone,
  then `idp consume` runs
- **THEN** Bronze holds exactly the records of the landing files, per tenant and
  entity

### Requirement: REQ-BRZ-04 — JSON:API source adapter
The ingestion layer SHALL provide an adapter that flattens TM Forum JSON:API usage
(voice and data) and recharge event envelopes into canonical records ready for
Bronze. The adapter SHALL:
- lift the `attributes` and `event-details` fields to top-level columns
- strip `UUID(...)` wrappers from the document id and relationship targets
- split charged amounts into `net_amount`, `tax_amount`, `gross_amount`
  (= net + tax) and `currency`

An absent money block SHALL yield zero amounts and no currency.

#### Scenario: SCN-BRZ-04.1 — Usage envelope flattened
- **WHEN** the data-usage sample in `data_samples/` is passed to the adapter
- **THEN** the record carries `service_id`, `event_type = data`, `qty` and `unit`
  from the used-service units, `gross_amount` = net + tax, and an `agreement_id`
  equal to the relationship target without its `UUID(...)` wrapper

#### Scenario: SCN-BRZ-04.2 — Voice details lifted
- **WHEN** the voice-usage sample is passed to the adapter
- **THEN** `traffic_case` and `other_party_number` from `event-details` appear as
  top-level fields

#### Scenario: SCN-BRZ-04.3 — Recharge envelope flattened
- **WHEN** the recharge sample is passed to the adapter
- **THEN** the record carries `service_id`, `amount`, `payment_method`, `status` and
  the unwrapped `agreement_id`

#### Scenario: SCN-BRZ-04.4 — Missing money block
- **WHEN** an envelope has no charged-amount block
- **THEN** net, tax and gross amounts are 0 and `currency` is empty
