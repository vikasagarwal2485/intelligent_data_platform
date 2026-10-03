# Platform deployment delta

## MODIFIED Requirements

### Requirement: REQ-PLT-11 — Trino lake catalog and read-only analytics identity
Trino SHALL expose an Iceberg catalog `lake` backed by the lake's Iceberg JDBC
catalog (the `iceberg` database on the platform Postgres, catalog name `lake`, the
same rows the stages commit to), with native S3 access to the layer buckets, so that the `bronze`,
`silver` and `gold` namespaces are queryable as schemas. Access control SHALL grant
the analytics service user (`idp-mcp`) `SELECT` on `lake.silver` and `lake.gold`
only, and deny everything else, including all writes and DDL. Trino SHALL be
reachable in-cluster at `trino.idp-platform.svc:8080`, and from the operator's host
through a port-forward target.

#### Scenario: SCN-PLT-11.1 — Lake tables visible in Trino
- **WHEN** the lake has Silver and Gold tables and Trino is installed with the new
  values
- **THEN** `SHOW TABLES FROM lake.silver` lists the Silver tables, and a `SELECT` on
  one returns the rows the stages committed

#### Scenario: SCN-PLT-11.2 — Analytics identity is read-only
- **WHEN** the `idp-mcp` user runs a `SELECT` on `lake.gold.credit_scores`, and then an
  `INSERT` or a `SELECT` on `lake.bronze.tmf_party`
- **THEN** the first succeeds, and the others are denied by access control
