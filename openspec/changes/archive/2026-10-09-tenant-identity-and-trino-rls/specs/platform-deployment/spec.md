# Platform deployment delta

## MODIFIED Requirements

### Requirement: REQ-PLT-11 — Trino lake catalog and read-only analytics identity
Trino SHALL expose an Iceberg catalog `lake` backed by the lake's Iceberg JDBC
catalog (the `iceberg` database on the platform Postgres, catalog name `lake`, the
same rows the stages commit to), with native S3 access to the layer buckets, so that the `bronze`,
`silver` and `gold` namespaces are queryable as schemas.

Trino SHALL authenticate every client request; a request carrying only a user name
(no credential) SHALL be rejected. It SHALL accept:
- **access tokens** issued by the platform realm, validated for signature (the
  realm's published keys), issuer, audience `trino` and expiry, the user being the
  token's `idp_principal` claim;
- **passwords** for the service users `admin` (operations) and `superset` (the BI
  tool) only, kept in a Kubernetes Secret.

Clients SHALL connect over HTTPS. Analytics identities are tenant principals whose
access is read-only and row-filtered (REQ-TEN-03): `SELECT` on `lake.silver` and
`lake.gold` only, and everything else denied, including all writes and DDL. There
is no shared analytics identity. Trino SHALL be reachable in-cluster at
`trino.idp-platform.svc:8443`, and from the operator's host through a port-forward
target.

#### Scenario: SCN-PLT-11.1 — Lake tables visible in Trino
- **WHEN** the lake has Silver and Gold tables and `admin` lists and reads them
- **THEN** `SHOW TABLES FROM lake.silver` lists the Silver tables, and a `SELECT` on
  one returns the rows the stages committed

#### Scenario: SCN-PLT-11.2 — Analytics identity is read-only
- **WHEN** a tenant principal runs a `SELECT` on `lake.gold.credit_scores`, and then an
  `INSERT` or a `SELECT` on `lake.bronze.tmf_party`
- **THEN** the first succeeds, and the others are denied by access control

#### Scenario: SCN-PLT-11.3 — A user name alone is not accepted
- **WHEN** a client sends a query with only a user name (e.g. `X-Trino-User: admin`),
  or a wrong password for `admin`
- **THEN** Trino rejects it as unauthenticated

#### Scenario: SCN-PLT-11.4 — Only HTTPS carries credentials
- **WHEN** a client sends a token or password to Trino over plain HTTP
- **THEN** the request is refused, and the same request over HTTPS is accepted
