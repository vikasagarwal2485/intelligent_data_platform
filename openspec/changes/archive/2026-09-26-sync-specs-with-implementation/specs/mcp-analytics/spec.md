# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-MCP-02 — Usage-events analytics tool
The system SHALL provide `analyze_usage_events_tool(tenant_id, ...)` that reads
`fact_usage` and returns:

- (a) total event count
- (b) **revenue per event type**: sum of `gross_amount` grouped by `event_type`
- (c) **duration/volume per event type**: voice seconds from `qty` where
  `unit=seconds`, data bytes where `unit=bytes`
- (d) **usage by event type over time**: counts and revenue bucketed by a
  `granularity` of `day`, `week`, `month` (default) or `quarter`

#### Scenario: SCN-MCP-02.1 — Revenue and duration by type
- **WHEN** the tool runs on a tenant with voice and data usage
- **THEN** `revenue_by_event_type` and `duration_by_event_type` each contain a
  `voice` and a `data` entry, and the event counts sum to the total

#### Scenario: SCN-MCP-02.2 — Time series present
- **WHEN** a time granularity of `day`, `week`, `month` or `quarter` is requested
- **THEN** the tool returns a time-bucketed series keyed by period and event type

### Requirement: REQ-MCP-06 — Read-only, tenant-scoped MCP exposure
The tools SHALL be exposed by an MCP server (`idp mcp`, stdio) as read-only tools.
Every tool SHALL require a `tenant_id` argument and return only that tenant's data.
No tool writes to the lake, and no tool returns raw (untokenized) subscriber
identifiers. In the following cases, every tool SHALL return a structured result
rather than raise:

- **Missing or unknown `tenant_id`**: the result lists the tenants present in the
  lake (`available_tenants`) and carries an `action_required` telling the client to
  ask the user which tenant to use rather than guess.
- **Lake unreachable** (connection or permission failure): the result carries an
  `error` and the lake `source`. This SHALL be distinct from a genuinely empty
  result.

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
- **WHEN** the lake endpoint cannot be reached
- **THEN** the tool returns an `error` naming the lake `source`, not a result with
  zero counts
