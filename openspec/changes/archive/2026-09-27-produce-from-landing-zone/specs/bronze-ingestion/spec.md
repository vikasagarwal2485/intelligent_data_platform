# Spec Delta

## MODIFIED Requirements

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
