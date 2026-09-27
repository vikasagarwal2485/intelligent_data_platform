# Tasks

## 1. Trino catalog and access control

- [ ] 1.1 Add the `lake` Iceberg catalog on the lake's JDBC catalog (`iceberg.catalog.type=jdbc`, the `iceberg` Postgres database, `catalog-name=lake`, connection and S3 credentials from Secrets, native S3 to MinIO) and file-based access control (`idp-mcp`: `SELECT` on `lake.silver.*` and `lake.gold.*` only) to `deploy/platform/trino/trino-values.yaml`, plus `make trino-api`. Verify on minikube (after 1.3) with SCN-PLT-11.1 (`SHOW TABLES FROM lake.silver`, `SELECT` returns rows; this also confirms the plugin's PostgreSQL driver) and SCN-PLT-11.2 / SCN-MCP-06.5 (insert and Bronze reads denied for `idp-mcp`)
- [ ] 1.2 Create `deploy/dev/trino-compose.yaml` (Trino 480 + Postgres + MinIO) using the same catalog file and access rules. Verify that `docker compose up -d --wait` is healthy and `SHOW SCHEMAS FROM lake` works
- [ ] 1.3 Pin the lake catalog name: `catalog.name` in config (`IDP_CATALOG_NAME`), defaulting to `lake` for non-SQLite catalogs, with `catalog.catalog_name(cfg)` returning it (local SQLite keeps the per-file hash). Add `make lake-catalog-rename`, an idempotent one-transaction `UPDATE … SET catalog_name='lake'` of the existing cluster rows. Verify with config and catalog unit tests (SCN-FND-01.7), then on minikube: pause the DAGs, rename, deploy the new image, and check that `make silver` processes 0 and every lake table still reads

## 2. Query layer

- [ ] 2.1 Add the `trino` config section (`host`, `port`, `user: idp-mcp`, `catalog: lake`, `http_scheme`) with `IDP_TRINO_*` overrides, and the `trino` dependency. Verify with config tests (SCN-FND-01.1, 01.6)
- [ ] 2.2 Create `src/idp/mcp/sql.py`: `connect`, `query(cfg, sql, params)` with DB-API binding, error mapping (connection → `LakeReadError(source=url)`, table-not-found → empty, access-denied → `LakeReadError`), and `available_tenants` from `$partitions`. Verify with unit tests using a fake connection

## 3. Tools as SQL

- [ ] 3.1 Move the current pandas tool logic to `tests/mcp_reference.py` (the oracle, unchanged). Verify that the oracle reproduces the current test expectations
- [ ] 3.2 Rewrite the subscribers, usage-events, recharges and fee-charges tools as SQL with Python shaping (granularity allow-list → SQL literal; period labels identical to today). Every result passes through `_scoped`. Verify with unit tests on canned query results (output keys and shapes unchanged)
- [ ] 3.3 Rewrite the recharge-anomalies tool (median/MAD via `approx_percentile`, robust z, limited spikes, subscriber-day volume spikes, periodic view). Verify with unit tests on canned results
- [ ] 3.4 Rewrite the fraud- and credit-score tools (per-window stats, `approx_percentile` array, `width_bucket` histograms, snapshot fallback, affordability gap). Verify with unit tests on canned results (SCN-MCP-07.2, 08.2 shapes)
- [ ] 3.5 Keep the tenant guard and error contract: a missing or unknown tenant → `available_tenants` + `action_required`; an injection string is treated as unknown; an unreachable Trino → `error` + `source`. Verify with unit tests for SCN-MCP-06.3, 06.4 and 06.6

## 4. Integration and parity

- [ ] 4.1 `tests/test_mcp_trino.py` (marker `trino`): build the lake through the real stages into the compose stack, then compare every tool with the oracle. Exact fields must be equal; rank statistics within ±1% rank; anomaly flags equal except within ±1% of the threshold. Verify SCN-MCP-09.2 and SCN-MCP-06.2 with `pytest -m trino`
- [ ] 4.2 Pushdown checks: `EXPLAIN` of each tool's queries scans only the tenant's partitions (SCN-MCP-09.3), and the rows fetched per call are bounded by the output shape (instrument `query()`; SCN-MCP-09.1)
- [ ] 4.3 CI: add the `mcp-trino` job (compose up, `pytest -m trino`), and exclude `trino` from the default job. Verify that both jobs are green on push

## 5. Operator experience and docs

- [ ] 5.1 Update `make mcp-config`, `make mcp-smoke` and the MCP server docstrings for Trino; retire `mcp-config-minio`. Verify with `make mcp-smoke` against minikube Trino, and that a Claude Desktop session lists all seven tools (SCN-MCP-06.1)
- [ ] 5.2 Update `README.md` (MCP section) and `deploy/README.md`, and add an ADR superseding ADR-0021 (MCP over Trino; read-only identity; approximation bound; dev auth caveat). Verify by reading

## 6. Verification

- [ ] 6.1 `pytest -q` (default job) is green, and `pytest -m trino` is green against the compose stack
- [ ] 6.2 On minikube, call every tool for both tenants through the MCP server. Verify the answers match `tests/mcp_reference.py` computed on the same lake, and that Trino's query history shows only `SELECT`s by `idp-mcp` against `lake.silver` / `lake.gold`
