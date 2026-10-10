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
`lake.gold` only, and everything else denied, including all writes and DDL, with
one exception for client metadata: read-only access to `system.jdbc` and
`system.metadata`, whose rows Trino filters by the principal's table access, so
BI clients can validate tables (never `system.runtime` or any other `system`
schema). There is no shared analytics identity. Every user other than `admin`
SHALL see and kill only their own queries: the query list of the REST API and
`system.runtime.queries` never show another user's query text. Trino SHALL be reachable in-cluster at
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

#### Scenario: SCN-PLT-11.5 — Only your own queries are visible
- **WHEN** `bob@tenant_b` has run a query and `alice@tenant_a` lists the queries
  through the REST API (`GET /v1/query`)
- **THEN** she sees only her own queries, never Bob's query text; `admin` sees all

#### Scenario: SCN-PLT-11.6 — Client metadata, filtered
- **WHEN** a tenant principal reads `system.jdbc.tables` and `system.metadata`, and then
  `system.runtime.queries`
- **THEN** the first two list only `lake.silver` and `lake.gold` tables (no Bronze), and
  the third is denied

## ADDED Requirements

### Requirement: REQ-PLT-12 — Apache Superset service
The platform SHALL deploy Apache Superset in `idp-platform` through the Apache
Superset Kubernetes Operator (pinned), as one `Superset` resource, with a pinned
image that adds the Trino and Postgres drivers and the OIDC client, with:
- its metadata in a `superset` database on the platform TimescaleDB, created if
  missing, and migrated whenever the image changes;
- no bundled Postgres or Redis, and no Celery workers or beat;
- its secret key, the OIDC client secret, the `superset` Trino password and the
  tenant roles' passwords from Kubernetes Secrets, never from the values file or the
  image;
- the sign-in configuration of REQ-BI-01, and both database connections of
  REQ-BI-05.

Superset SHALL be reachable from the operator's host through a port-forward target,
and the documented host setup SHALL complete the Keycloak sign-in in a browser.

#### Scenario: SCN-PLT-12.1 — Installed and reachable
- **WHEN** the Superset target runs on a cluster with the platform services
- **THEN** Superset becomes ready, its metadata lives in the `superset` database, and
  `http://localhost:8088` offers the Keycloak sign-in

#### Scenario: SCN-PLT-12.2 — No secret in the repository
- **WHEN** the deployment files and the image are searched for the secret key, the
  `superset` Trino password and the database passwords
- **THEN** none is found; they come from Secrets. The only credential in the
  repository is the realm's documented dev-default `idp-superset` client secret, which
  `IDP_KC_SECRET_IDP_SUPERSET` replaces, as for every realm client
