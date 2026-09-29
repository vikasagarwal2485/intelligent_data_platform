# Proposal

## Why

The seven MCP analytics tools compute their answers in the MCP server process with
pandas. Even with the tenant filter that `iceberg-incremental-lakehouse` pushes into
the table scan, every tool still pulls **all of one tenant's rows** into memory
before aggregating. Examples:
- `analyze_usage_events_tool` loads every usage event of the tenant.
- The score tools load every score row within 30 days.

For an operator with ~250 M usage events a day, one tool call would pull billions of
rows into the MCP process.

Trino is deployed for exactly this: interactive SQL over the lake with filters and
aggregation pushed down. It has no catalog configured, and ADR-0021 deferred it
("pandas over Silver … keeps the analytics surface unit-testable without a
cluster").

This is change 4 of the lakehouse roadmap. It requires
`iceberg-incremental-lakehouse`, which provides the Iceberg tables and their JDBC
catalog (the `iceberg` database on the platform Postgres, ADR-0028).

## What Changes

- **Trino gets an Iceberg catalog `lake`** on the lake's Iceberg JDBC catalog (the
  same Postgres `iceberg` database the stages use), with native S3 access to MinIO.
  - Trino must address the catalog rows by name, so the cluster catalog's name is
    pinned to `lake` (`catalog.name`, `IDP_CATALOG_NAME`) instead of being derived
    from the connection URI. The existing rows are renamed once. The MCP service user `idp-mcp` is limited by file-based access
  control to `SELECT` on `lake.silver` and `lake.gold`, so read-only is enforced by
  the engine, not only by code.
- **The seven tools become SQL.** Each tool runs a small number of parameterized
  queries against `lake`, with `tenant_id` always a bound parameter. Grouping, sums,
  windows, histograms, medians and percentiles (from window-function order
  statistics) run inside Trino, and only compact results come back. Every returned row still
  carries `tenant_id` and passes through the tenant gateway (`scope_to_tenant`)
  before it's used (REQ-FND-06).
- **Output shapes are unchanged:** the JSON keys and the meaning of every field, the
  tenant guard (`available_tenants`, `action_required`), the isolation errors, and the
  unreachable-lake error. The unreachable case now names the Trino endpoint as
  `source`.
- **Rank statistics** (percentiles, medians, and the anomaly tool's MAD) may become
  engine-computed approximations with a bounded rank error (REQ-MCP-09); the
  implementation computes them exactly (design, Risks). Counts, sums, means and
  histograms stay exact.
- **Tests:**
  - The pandas logic that exists today moves to the test suite, as the reference
    oracle.
  - An integration suite runs the SQL tools against a real Trino and compares them
    with the oracle on the same lake. The Trino is a disposable Trino + Postgres +
    MinIO compose stack, locally and in CI.
  - Unit tests cover the SQL building and result shaping without Trino.
- ADR-0021 is superseded by an ADR for MCP over Trino.

Non-goals:
- Per-end-user identity or row-level security inside Trino. There's one MCP service
  user, with tenant scoping in the tools and the gateway.
- The streamable-HTTP MCP deployment.
- Exposing raw SQL to the MCP client.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `mcp-analytics`: REQ-MCP-06 (tools query the engine; read-only enforced by access
  control; unreachable engine as a structured error); new REQ-MCP-09 (pushdown and
  bounded results; approximation bound for rank statistics).
- `foundation`: REQ-FND-01 (the `trino` config section and `IDP_TRINO_*` env; a
  stable catalog name, `IDP_CATALOG_NAME`).
- `platform-deployment`: new REQ-PLT-11 (Trino `lake` catalog on the lake's JDBC
  catalog, read-only MCP user).
- `orchestration`: REQ-ORC-05. Since `distributed-spark-runtime`, it lists the only
  permitted pandas uses, and the MCP tools aren't among them. After this change the
  tools use pandas only to shape the small results of engine queries, so the list
  must gain "shaping the bounded results of analytics queries (REQ-MCP-09)".
  The delta (`specs/orchestration/spec.md`) carries REQ-ORC-05 from the main spec
  with that one addition.

## Impact

- **Code:**
  - `src/idp/mcp/tools.py` rewritten over a small query layer
    (`src/idp/mcp/sql.py`: connection, parameter binding, error mapping)
  - `src/idp/common/config.py` (`trino` section; `catalog.name`)
  - `src/idp/common/catalog.py` (`catalog_name` uses the configured name)
  - `src/idp/mcp/server.py` (unchanged tool signatures)
- **Dependencies:** `trino` (Python DB-API client).
- **Deploy:**
  - `deploy/platform/trino/trino-values.yaml` (catalog, access-control rules, the
    `idp-mcp` user)
  - `make trino-api` (port-forward for the host MCP server)
  - `mcp-config` targets updated
  - `deploy/dev/trino-compose.yaml` for tests
- **CI:** a second job, `mcp-trino`, brings up the compose stack and runs
  `pytest -m trino`. The default test job skips those tests.
- **Tests:** `tests/test_mcp_tools.py` splits into unit tests (no Trino) and
  `tests/test_mcp_trino.py` (integration against the oracle).
