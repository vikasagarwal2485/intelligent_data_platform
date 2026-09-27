# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-MCP-06 — Read-only, tenant-scoped MCP exposure
The tools SHALL be exposed by an MCP server (`idp mcp`, stdio) as read-only tools.
Every tool SHALL require a `tenant_id` argument and return only that tenant's data.
Tools SHALL answer by querying the lake through the SQL query engine (Trino) with
the tenant as a bound query parameter. Every returned row SHALL pass through the
tenant gateway before use. The engine SHALL enforce read-only access for the MCP
service identity, so no tool can write to the lake. No tool returns raw
(untokenized) subscriber identifiers. In the following cases, every tool SHALL
return a structured result rather than raise:

- **Missing or unknown `tenant_id`**: the result lists the tenants present in the
  lake (`available_tenants`) and carries an `action_required` telling the client to
  ask the user which tenant to use rather than guess.
- **Lake unreachable** (query engine down, connection or permission failure): the
  result carries an `error` and the `source` (the query engine endpoint). This SHALL
  be distinct from a genuinely empty result.

#### Scenario: SCN-MCP-06.1 — Tools discoverable over MCP
- **WHEN** an MCP client lists tools against the running server
- **THEN** all seven `analyze_*_tool` tools are advertised with descriptions and
  input schemas

#### Scenario: SCN-MCP-06.2 — Tenant isolation enforced
- **WHEN** a tool is called with `tenant_id = tenant_a`
- **THEN** the result reflects only `tenant_a` rows, and identifiers in the output
  are the tokenized `service_id` values (no raw MSISDN/IMSI)

#### Scenario: SCN-MCP-06.3 — Missing tenant handled
- **WHEN** a tool is called with an unknown or missing `tenant_id`
- **THEN** the tool returns an error result with `available_tenants` and
  `action_required`, rather than raising, and never leaks another tenant's data

#### Scenario: SCN-MCP-06.4 — Unreachable lake is not "zero"
- **WHEN** the query engine cannot be reached
- **THEN** the tool returns an `error` naming the engine endpoint as `source`, not a
  result with zero counts

#### Scenario: SCN-MCP-06.5 — Writes are refused by the engine
- **WHEN** a statement that writes (e.g. `INSERT`, `DELETE`, `CREATE`) is sent to the
  lake catalog as the MCP service identity
- **THEN** the engine rejects it with an access-denied error

#### Scenario: SCN-MCP-06.6 — Tenant is a bound parameter
- **WHEN** a tool is called with a `tenant_id` containing SQL metacharacters (e.g.
  `x' OR '1'='1`)
- **THEN** it is treated as an unknown tenant, returns the missing-tenant result,
  and no other tenant's data is returned

## ADDED Requirements

### Requirement: REQ-MCP-09 — Aggregation pushed down to the query engine
Each tool SHALL compute its result inside the query engine: tenant and time
filters, grouping, sums, counts, means, histograms, and rank statistics. It SHALL
retrieve only the aggregated or explicitly limited rows it returns, never a tenant's
full table. The data read by a tool call SHALL therefore be bounded by the engine's
pruning of the tenant's partitions, and its result size by the tool's output shape
and its `limit`. Counts, sums, means and histogram bucket counts SHALL be exact.
Percentiles, medians and median absolute deviations MAY be approximate with a rank
error of at most 1%.

#### Scenario: SCN-MCP-09.1 — Only aggregates cross the wire
- **WHEN** `analyze_usage_events_tool` runs for a tenant with 1 000 000 usage events
- **THEN** the rows fetched from the engine number at most the tool's output
  entries (event types × periods, plus totals), not the event count

#### Scenario: SCN-MCP-09.2 — Exact where it matters
- **WHEN** a tool's result is compared with an exact in-memory computation on the
  same data
- **THEN** counts, sums, means and histogram counts are equal, and each reported
  percentile or median lies within ±1% rank of the exact value

#### Scenario: SCN-MCP-09.3 — Partition pruning by tenant
- **WHEN** a tool runs for `tenant_a` on a lake holding several tenants
- **THEN** the engine's query plan scans only `tenant_a`'s partitions of the tables
  it reads
