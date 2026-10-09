# Proposal

## Why

Tenant isolation on the analytics path rests on one control: the `WHERE tenant_id = ?`
each MCP tool writes. The tenant itself is a tool argument chosen by the caller, and
Trino authenticates nobody: any client on the network can send `X-Trino-User: idp-mcp`
(or `admin`) and read every tenant's rows. Keycloak already issues tenant-scoped
tokens, but only the scoring API checks them.

The target is defence in depth for the two analytics consumers we plan for, the MCP
server (conversational BI) and Apache Superset (reports), both reading the lake
through Trino. This change builds the two layers that every design needs first:
**identity** (Keycloak, with the tenant carried by the token all the way to Trino)
and **storage** (Trino row-level security on `tenant_id`). Each must refuse a
cross-tenant read on its own, so a bug in a tool's SQL no longer exposes other
tenants. A semantic layer (Cube) is not planned; Superset and the MCP server stay
the only consumers.

## What Changes

- **Keycloak tenant identities.**
  - Every token that may reach Trino carries a principal claim `idp_principal` of
    the form `<name>@<tenant_id>`, next to the existing `tenant_id` claim.
  - One group per tenant, and one MCP service client per tenant
    (`idp-mcp-<tenant>`, client credentials, audience `trino`).
  - A Superset login client (`idp-superset`, authorization-code flow), so Superset
    can be added without realm changes.
  - The tenant parts of the realm are **rendered from the configured tenants**
    (`idp keycloak-realm`), and a test checks the committed realm matches, so adding
    a tenant is one config entry plus a render.
- **Trino authentication** (**BREAKING** for Trino clients):
  - JWT authentication against the Keycloak realm's keys (issuer and audience
    checked), with the principal taken from `idp_principal`.
  - Password authentication for two service users only: `admin` (operations) and
    `superset`, which may **impersonate** tenant principals (`<name>@<tenant>`) and
    nobody else.
  - HTTPS on the coordinator, with a generated certificate locally.
  - The header-only authentication and the shared `idp-mcp` user are removed.
- **Trino row-level security:**
  - One rule for every tenant principal: `SELECT` on `lake.silver` and `lake.gold`
    with the row filter `tenant_id = <the principal's tenant>`, derived from
    `current_user`.
  - Iceberg metadata tables (`$partitions`, `$files`, `$snapshots`, …) are denied to
    tenant principals, because row filters don't apply to them.
  - A table without `tenant_id` fails the query instead of returning rows.
  - The read-only and Bronze-deny rules stay.
- **MCP server takes its tenant from its token:**
  - It obtains a token with its tenant client's credentials, refreshes it, and
    sends it to Trino.
  - A tool's `tenant_id` argument becomes optional: omitted means the token's
    tenant; a different tenant is refused without querying.
  - `available_tenants` lists only the caller's tenant.
- **Cross-tenant leakage tests per layer:** with each layer's check bypassed in
  turn, the other layer alone still refuses another tenant's rows. They run against
  the compose Trino stack in CI with real JWTs.
- **Performance assessment:** the MCP tools' query latency is measured with and
  without the row filter, against a budget of at most a 10% p95 increase, and the
  partition-pruning check confirms the filter still prunes by tenant.

## Capabilities

### New Capabilities
- `tenant-isolation`: the layered tenant-isolation model across the analytics path:
  tenant principals and their provisioning from config, Trino row-level security and
  metadata-table denial, independent enforcement verified by leakage tests, and the
  performance budget for the row filter.

### Modified Capabilities
- `platform-deployment`: REQ-PLT-11 changes: Trino requires authentication (JWT from
  Keycloak, password for the service users, HTTPS), and the shared `idp-mcp`
  identity is replaced by per-tenant principals.
- `serving-api`: REQ-SRV-03 and REQ-SRV-04 change: the serving lookup becomes
  `GET /serving/{table}/{service_id}`, the tenant comes from the Bearer token, and
  an optional `X-Tenant-Id` header is validated against it (**BREAKING** for the old
  path).
- `foundation`: REQ-FND-01 changes: `IDP_TRINO_USER` is dropped, and
  `IDP_TRINO_CA_CERT` and the MCP identity's `IDP_MCP_*` overrides are added.
- `mcp-analytics`: REQ-MCP-06 changes: the MCP server authenticates with a tenant
  service client, its tenant comes from the token, the `tenant_id` argument becomes
  optional and must match, and `available_tenants` lists only the caller's tenant.

## Non-goals

- Deploying Superset. This change provides its Keycloak client, its Trino service
  user and the impersonation rule; installing and configuring Superset is a
  follow-up change.
- A semantic layer (Cube Core).
- Row-level security in TimescaleDB for the scoring API's serving reads (the API
  already scopes by the token's tenant), and isolation in MinIO, Kafka or the
  Iceberg catalog, whose credentials stay shared by the pipeline stages.
- Provisioning beyond identity and Trino: topics, tenant namespaces and PII salts
  still come from their existing files.

## Impact

- **Keycloak:** `deploy/platform/keycloak/idp-realm.json` (rendered tenant section),
  `token.sh` (MCP and principal tokens). New `idp keycloak-realm` command
  (`src/idp/identity/`).
- **Trino:** `deploy/platform/trino/trino-values.yaml` (HTTPS, authentication, shared
  secret), `rules.json` (row filter, metadata-table deny, impersonation), a password
  file and TLS certificate as Kubernetes Secrets (new `make trino-certs`), and the
  `make lake-table-*` targets, which now authenticate as `admin`.
- **MCP:** `src/idp/mcp/sql.py` (JWT authentication, HTTPS), `tools.py` (tenant from
  the token, discovery), new `src/idp/mcp/auth.py` (client-credentials token
  provider), `config/platform.yaml` (`trino` and `mcp.auth` sections),
  `make mcp-config`.
- **Tests:** `tests/test_mcp_trino.py` and the compose stack
  (`deploy/dev/trino-compose.yaml`: JWT keys for tests, password file, rules), a new
  leakage suite `tests/test_tenant_isolation.py`, `tests/test_mcp_tools.py`,
  `tests/test_config.py`.
- **Docs:** README 1.5/1.6 and 4.6, `deploy/README.md`, the decision log (new ADR,
  ADR-0005 and ADR-0030 status).
- **Operations:** after upgrading, the MCP host config needs the tenant client's
  credentials (`make mcp-config TENANT=…`), and anyone using `X-Trino-User` must
  switch to a token or the `admin` password.
