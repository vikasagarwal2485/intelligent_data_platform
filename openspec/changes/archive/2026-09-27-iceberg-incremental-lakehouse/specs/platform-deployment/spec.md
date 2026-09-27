# Spec Delta

## ADDED Requirements

### Requirement: REQ-PLT-09 — Persistent Iceberg catalog (JDBC on the platform Postgres)
The platform SHALL provide the lake's Iceberg catalog as an Iceberg JDBC catalog
stored in the in-cluster TimescaleDB/Postgres, database `iceberg`, created
idempotently by the platform install. The stage jobs, the scoring API and the DAG
pods SHALL reach it with the catalog env (`IDP_CATALOG_TYPE=sql`, `IDP_CATALOG_URI`
set to that database). The catalog SHALL keep every table's full snapshot history,
because the incremental Silver checkpoints reference Bronze snapshots by id. Table
metadata and data SHALL stay in the bucket of their layer (`idp-bronze`,
`idp-silver`, `idp-gold`). Restarting any catalog client SHALL NOT lose any table,
namespace or snapshot.

#### Scenario: SCN-PLT-09.1 — Catalog state is durable
- **WHEN** the lake has tables and the pods that use the catalog are restarted
- **THEN** every namespace, table and snapshot is still listed, and the stages read
  and write them unchanged

#### Scenario: SCN-PLT-09.2 — Catalog reachable from the stages
- **WHEN** a stage job in `idp-app` starts with the catalog env
- **THEN** it can create, read and commit tables in `bronze`, `silver` and `gold`,
  from both Spark and the Python readers and writers

#### Scenario: SCN-PLT-09.3 — Snapshot history is kept
- **WHEN** a Bronze table receives several appends between two Silver runs
- **THEN** the catalog lists one snapshot per append, and the next incremental Silver
  run resumes from its checkpoint and reads exactly the appended records
