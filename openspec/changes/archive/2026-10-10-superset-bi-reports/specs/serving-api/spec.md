# Serving API delta

## MODIFIED Requirements

### Requirement: REQ-SRV-01 — Gold serving materialization (TimescaleDB)
The system SHALL provide a `publish` stage that reads the Gold score and feature
tables (`credit_scores`, `fraud_scores`, `credit_features`, `fraud_features`) from
the lake with the Spark engine and writes them to TimescaleDB `gold.*` tables
indexed by `(tenant_id, service_id)` for low-latency serving (ADR-0002). Rows SHALL
be loaded in parallel into a staging table, and each serving table SHALL then be
swapped into place in a single transaction, so readers never see a partly written
table. The swap SHALL carry the table's access controls with it: within the same
transaction, row-level security is enabled on the new table, its tenant policy for
the BI readers (REQ-BI-03) is created, and the readers' `SELECT` grant is made, so
there is no moment when a BI reader can see the table without its policy. Concurrent publishers of the same table SHALL be serialized rather than fail.
It SHALL be an independent stage (`idp publish`), run on demand or on a schedule,
and SHALL fail clearly when Gold has no tables to publish.

#### Scenario: SCN-SRV-01.1 — Publish Gold to TimescaleDB
- **WHEN** `idp publish` runs against a populated Gold layer
- **THEN** the Gold score/feature tables are written to TimescaleDB `gold.*`,
  indexed by `(tenant_id, service_id)`, and are queryable by that key

#### Scenario: SCN-SRV-01.2 — Independent of the API
- **WHEN** the publish stage runs
- **THEN** it performs no HTTP serving and does not require the scoring API

#### Scenario: SCN-SRV-01.3 — Concurrent publishers do not collide
- **WHEN** two publish runs replace the same serving table at the same time
- **THEN** both complete without a catalog/duplicate-type error, one after the other

#### Scenario: SCN-SRV-01.4 — Nothing to publish
- **WHEN** `idp publish` runs and Gold contains none of the serving tables
- **THEN** it fails with an error instructing to run the Gold stage first

#### Scenario: SCN-SRV-01.5 — Readers see old or new, never partial
- **WHEN** a client reads `gold.credit_scores` while publish is loading a new run
- **THEN** it sees the complete previous table until the swap commits, and the
  complete new table afterwards

#### Scenario: SCN-SRV-01.6 — Access controls survive a publish
- **WHEN** `idp publish` replaces `gold.credit_scores` while a `tenant_a` BI role is
  reading it
- **THEN** that role reads only `tenant_a` rows before and after the swap, and never
  gets a permission error or another tenant's rows
