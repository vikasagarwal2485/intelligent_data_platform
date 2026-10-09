# MCP analytics delta

## MODIFIED Requirements

### Requirement: REQ-MCP-06 — Read-only, tenant-scoped MCP exposure
The tools SHALL be exposed by an MCP server (`idp mcp`, stdio) as read-only tools.

**The server's tenant comes from its identity.** Against the cluster's query engine,
the server SHALL authenticate with its tenant's MCP service client (client
credentials) and use the resulting access token for every query, obtaining a new
token before the current one expires. The tenant it serves is the token's
`tenant_id` claim, and it SHALL answer only for that tenant:
- `tenant_id` is an **optional** tool argument: when omitted, the token's tenant is
  used;
- a `tenant_id` that differs from the token's tenant SHALL get the refusal result
  below, without querying.

**Queries.** Tools SHALL answer by querying the lake through the SQL query engine
(Trino) with the tenant as a bound query parameter. The engine additionally
row-filters every query to the token's tenant (REQ-TEN-03). Every returned row SHALL
pass through the tenant gateway before use. The engine SHALL enforce read-only
access for the MCP identity, so no tool can write to the lake. No tool returns raw
(untokenized) subscriber identifiers.

In the following cases, every tool SHALL return a structured result rather than
raise:

- **Missing tenant in the lake, or another tenant requested**: the result lists the
  tenants the caller may use (`available_tenants`: only the token's tenant, if it has
  data) and carries an `action_required` telling the client to ask the user rather
  than guess.
- **Lake unreachable** (query engine down, connection or permission failure): the
  result carries an `error` and the `source` (the query engine endpoint). This SHALL
  be distinct from a genuinely empty result.
- **Identity unavailable** (the identity provider unreachable, or the client
  credentials rejected): the result carries an `error` and the `source` (the identity
  provider endpoint), and no query is sent.

#### Scenario: SCN-MCP-06.1 — Tools discoverable over MCP
- **WHEN** an MCP client lists tools against the running server
- **THEN** all seven `analyze_*_tool` tools are advertised with descriptions and
  input schemas, with `tenant_id` optional

#### Scenario: SCN-MCP-06.2 — Tenant isolation enforced
- **WHEN** a tool is called on a server holding a `tenant_a` token, with
  `tenant_id = tenant_a` or without `tenant_id`
- **THEN** the result reflects only `tenant_a` rows, and identifiers in the output
  are the tokenized `service_id` values (no raw MSISDN/IMSI)

#### Scenario: SCN-MCP-06.3 — Missing tenant handled
- **WHEN** a tool is called with an unknown tenant, or with `tenant_b` on a server
  holding a `tenant_a` token
- **THEN** the tool returns an error result with `available_tenants` (at most
  `tenant_a`) and `action_required`, rather than raising, sends no query, and never
  leaks another tenant's data

#### Scenario: SCN-MCP-06.4 — Unreachable lake is not "zero"
- **WHEN** the query engine cannot be reached, or the identity provider can't issue a
  token
- **THEN** the tool returns an `error` naming the unreachable endpoint as `source`, not
  a result with zero counts

#### Scenario: SCN-MCP-06.5 — Writes are refused by the engine
- **WHEN** a statement that writes (e.g. `INSERT`, `DELETE`, `CREATE`) is sent to the
  lake catalog with the MCP server's token
- **THEN** the engine rejects it with an access-denied error

#### Scenario: SCN-MCP-06.6 — Tenant is a bound parameter
- **WHEN** a tool is called with a `tenant_id` containing SQL metacharacters (e.g.
  `x' OR '1'='1`)
- **THEN** it is treated as an unknown tenant, returns the missing-tenant result,
  and no other tenant's data is returned

#### Scenario: SCN-MCP-06.7 — Expiring token is renewed
- **WHEN** the server's access token is about to expire between two tool calls
- **THEN** the second call obtains a new token and succeeds, without a restart
