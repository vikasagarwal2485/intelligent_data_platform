# Tenant isolation delta

## MODIFIED Requirements

### Requirement: REQ-TEN-03 — Row-level security in the query engine
For a tenant principal, the query engine SHALL return only rows whose `tenant_id`
equals the principal's tenant, from every table in `lake.silver` and `lake.gold`,
whatever the query's own filters. In addition:
- **Metadata tables** (Iceberg `$partitions`, `$files`, `$snapshots`, `$manifests`,
  `$history`, `$properties` and the like) SHALL be denied to tenant principals,
  since row filters don't apply to them.
- **A table without a `tenant_id` column** SHALL fail the query rather than return
  rows.
- **A principal that isn't of the form `<name>@<configured tenant>`** SHALL have no
  access to the lake.
- **Impersonation:** the BI service user (`superset`) MAY run a query as a tenant
  principal (impersonation), and the query is then filtered for that principal. It
  SHALL NOT impersonate any other user, and SHALL NOT read the lake as itself.
- **Read-only:** tenant principals SHALL NOT write, run DDL, or read
  `lake.bronze`.
- **Queries:** a tenant principal SHALL see and kill only its own queries, so no
  other tenant's query text (with its literal values) is ever visible to it.

#### Scenario: SCN-TEN-03.1 — Unfiltered query sees one tenant
- **WHEN** `mcp@tenant_a` runs `SELECT tenant_id, count(*) FROM lake.silver.dim_subscriber
  GROUP BY 1`
- **THEN** the only `tenant_id` returned is `tenant_a`

#### Scenario: SCN-TEN-03.2 — Asking for another tenant returns nothing
- **WHEN** `mcp@tenant_a` runs `SELECT count(*) FROM lake.gold.credit_scores WHERE
  tenant_id = 'tenant_b'`
- **THEN** the count is 0

#### Scenario: SCN-TEN-03.3 — Metadata tables denied
- **WHEN** `mcp@tenant_a` selects from `lake.silver."dim_subscriber$partitions"` or
  `lake.gold."credit_scores$files"`
- **THEN** access is denied

#### Scenario: SCN-TEN-03.4 — Table without tenant_id fails closed
- **WHEN** a Silver table without a `tenant_id` column exists and a tenant principal
  selects from it
- **THEN** the query fails and returns no rows

#### Scenario: SCN-TEN-03.5 — Unknown principal has no access
- **WHEN** a valid token's principal is `someone` (no tenant) or
  `someone@tenant_x` (no such tenant)
- **THEN** every query on the lake is denied or returns no rows

#### Scenario: SCN-TEN-03.6 — Impersonation is filtered and bounded
- **WHEN** `superset` runs a query on `lake.gold.fraud_scores` impersonating
  `alice@tenant_b`, then tries to impersonate `admin`, then queries as itself
- **THEN** the first returns only `tenant_b` rows, and the other two are denied

#### Scenario: SCN-TEN-03.7 — Another tenant's query text is invisible
- **WHEN** `bob@tenant_b` runs a query naming a `tenant_b` service id, and
  `alice@tenant_a` lists queries through the REST API
- **THEN** Bob's query is not in her list

## ADDED Requirements

### Requirement: REQ-TEN-06 — The BI path holds isolation in each layer
On the BI path, each layer SHALL refuse a cross-tenant read on its own, and an
automated suite SHALL verify it:
- **identity:** the tenant Superset uses for a query (the impersonated Trino
  principal, the TimescaleDB role) comes only from the signed-in user's token; a
  user without a tenant gets no connection at all;
- **storage, Silver:** Trino's row filter limits the impersonated principal
  (REQ-TEN-03);
- **storage, Gold:** TimescaleDB's row-level security limits each tenant role, even
  for a query without any tenant filter, and the role can't switch to another
  tenant's role.

The identity mapping and the Gold policies SHALL be tested without a cluster (a real
Postgres for the policies); the end-to-end path SHALL be checked on the cluster by
signing in as users of two tenants.

#### Scenario: SCN-TEN-06.1 — Gold policy holds without a filter
- **WHEN** `idp_bi_tenant_a` runs `SELECT DISTINCT tenant_id FROM gold.credit_scores`
- **THEN** the only value is `tenant_a`

#### Scenario: SCN-TEN-06.2 — The connection follows the user, not the input
- **WHEN** Superset prepares a TimescaleDB or Trino connection for a `tenant_a` user,
  whatever the query or chart parameters say
- **THEN** it uses `idp_bi_tenant_a`, or impersonates `<name>@tenant_a`; for a user
  without a tenant it refuses to connect

#### Scenario: SCN-TEN-06.3 — Two tenants, same dashboard, on the cluster
- **WHEN** users of `tenant_a` and `tenant_b` sign in and open both starter dashboards
- **THEN** every chart shows only the signed-in user's tenant
