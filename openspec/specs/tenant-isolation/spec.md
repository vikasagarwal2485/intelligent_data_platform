# Tenant Isolation Specification

## Purpose

Isolates tenants on the analytics path with independent layers: an identity layer
(Keycloak tokens carrying the tenant to every query) and a storage layer (Trino
row-level security on `tenant_id`). Each refuses a cross-tenant read on its own.

## Requirements

### Requirement: REQ-TEN-01 — Tenant principals in identity tokens
Every identity that can query the lake through Trino SHALL be a **tenant principal**
named `<name>@<tenant_id>`, where `<tenant_id>` is a configured tenant. Its access
tokens, issued by the platform realm, SHALL carry:
- the principal in an `idp_principal` claim;
- the tenant in the `tenant_id` claim, equal to the principal's suffix;
- the audience `trino` when the token is meant for Trino.

The realm SHALL hold, per configured tenant:
- a group for that tenant's users;
- an MCP service client (`idp-mcp-<tenant_id>`, client credentials) whose tokens
  carry the principal `mcp@<tenant_id>`.

The realm SHALL also hold an interactive login client for the BI tool (`idp-superset`)
whose tokens carry the user's principal and tenant. The principal and tenant claims
SHALL come from the realm's configuration, never from anything the user or client
can set.

#### Scenario: SCN-TEN-01.1 — User token carries a consistent principal
- **WHEN** a user in the `tenant_a` group obtains a token for Trino
- **THEN** it carries `idp_principal = <username>@tenant_a`, `tenant_id = tenant_a` and
  the audience `trino`

#### Scenario: SCN-TEN-01.2 — MCP service token
- **WHEN** the client `idp-mcp-tenant_b` obtains a token with its client credentials
- **THEN** the token carries `idp_principal = mcp@tenant_b`, `tenant_id = tenant_b`
  and the audience `trino`

#### Scenario: SCN-TEN-01.3 — Claims can't be self-asserted
- **WHEN** a user changes their own profile attributes, or a client requests extra
  claims
- **THEN** the `idp_principal` and `tenant_id` claims in their next token are
  unchanged

### Requirement: REQ-TEN-02 — Tenant identities provisioned from configuration
The tenant-specific part of the realm (the groups, MCP service clients and their
claim mappers) SHALL be generated from the configured tenant list, so adding a
tenant needs only its configuration entry and a regeneration. The realm committed to
the repository SHALL equal the generated one. The Trino access rules SHALL NOT
contain per-tenant entries, so a new tenant needs no Trino change.

#### Scenario: SCN-TEN-02.1 — Committed realm matches the tenants
- **WHEN** the realm is regenerated from the configured tenants
- **THEN** it equals the committed realm file

#### Scenario: SCN-TEN-02.2 — Adding a tenant
- **WHEN** a tenant `tenant_c` is added to the configuration and the realm is
  regenerated and re-imported
- **THEN** the realm has a `tenant_c` group and an `idp-mcp-tenant_c` client, a token
  from that client reads only `tenant_c` rows through Trino, and the Trino rules file
  is unchanged

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

### Requirement: REQ-TEN-04 — Each layer holds on its own
The identity layer and the storage layer SHALL each refuse a cross-tenant read when
the other one fails. An automated suite SHALL verify this against the real query
engine with real signed tokens, and SHALL run in CI:
- **storage alone:** a tenant principal's query without any tenant filter, and an
  MCP tool whose own tenant filter is removed, return only the principal's tenant;
- **identity alone:** an MCP server holding a `tenant_a` token refuses a request for
  `tenant_b` before any query, even against an engine that would return
  `tenant_b` rows;
- **forged identity:** the engine rejects tokens that are unsigned, signed by an
  unknown key, expired, for another audience, or without a principal claim, and
  requests with no token at all.

#### Scenario: SCN-TEN-04.1 — Storage holds without the query filter
- **WHEN** an MCP tool is run with its `tenant_id` predicate removed, as `mcp@tenant_a`
- **THEN** its result contains only `tenant_a` data

#### Scenario: SCN-TEN-04.2 — Identity holds without the storage filter
- **WHEN** an MCP server with a `tenant_a` token is asked for `tenant_b`, and its query
  engine is one that would return every tenant's rows
- **THEN** it returns the refusal result and sends no query

#### Scenario: SCN-TEN-04.3 — Forged or missing tokens are rejected
- **WHEN** a query is sent with no token, an expired token, a token signed by an
  unknown key, a token for another audience, or a token without `idp_principal`
- **THEN** the engine rejects it as unauthenticated and returns no data

### Requirement: REQ-TEN-05 — Row-filter performance budget
Row-level security SHALL NOT raise the median (p50) latency of the MCP tools' queries
by more than 10% over the same queries without it, measured on the reference test
dataset with at least 60 runs per tool, alternating filtered and unfiltered runs.
The p95 latency SHALL be measured and reported alongside, not gated: on a shared
host its run-to-run variance exceeds the budget. A filtered query SHALL still read
only the principal's tenant partitions.

#### Scenario: SCN-TEN-05.1 — Latency within budget
- **WHEN** every MCP tool is run at least 60 times as a tenant principal, alternating
  with and without the row filter, on the reference dataset
- **THEN** the median latency with the filter is at most 1.10 times the median
  without it, and the p50 and p95 measurements are recorded

#### Scenario: SCN-TEN-05.2 — Filter still prunes by tenant
- **WHEN** the query plan of a tool's query is inspected as `mcp@tenant_a`
- **THEN** every table scan is constrained to `tenant_id = 'tenant_a'`
