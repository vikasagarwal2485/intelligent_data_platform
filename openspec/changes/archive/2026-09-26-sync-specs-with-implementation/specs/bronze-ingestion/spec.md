# Spec Delta

## ADDED Requirements

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
