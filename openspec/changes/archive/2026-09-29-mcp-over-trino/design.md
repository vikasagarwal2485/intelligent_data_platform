# Design

## Context

- **The tools today** (`src/idp/mcp/tools.py`, about 600 lines) are seven functions,
  each reading its tables into pandas, then aggregating:
  - distributions use `quantile` and `np.histogram`
  - the anomaly tool computes median/MAD and mean + k·std over subscriber-days
  - periods come from `_period` (day/week/month/quarter)
- **After `iceberg-incremental-lakehouse`,** reads go through `pyiceberg` with a
  `tenant_id` filter, but the whole tenant slice still lands in the MCP process.
- **The tenant guard** (`_lake_guard`) and the gateway (`_scoped` / `scope_to_tenant`)
  define the error contract that REQ-MCP-06 and REQ-FND-06 pin.
- **Trino 480** is installed coordinator-only (`trino-values.yaml`, 1 G heap), with no
  catalogs. The lake's tables are in an Iceberg **JDBC catalog**: the `iceberg`
  database on the platform TimescaleDB/Postgres (ADR-0028; Nessie was rejected
  because its REST catalog keeps only the current snapshot). Every JDBC-catalog row
  carries a catalog name. Today that name is `lake_<sha1(uri)[:10]>`, and in the
  cluster the hashed URI includes the database password.
- **The MCP server** runs on the operator's host (Claude Desktop, stdio) and reaches
  cluster services through port-forwards (`make minio-api`); it will reach Trino the
  same way.

## Goals / Non-Goals

**Goals:**
- Every tool's work runs in Trino; only compact results reach the MCP process.
- The output contract is unchanged, including the guard and error shapes.
- Read-only is enforced by the engine. The tenant is always a bound parameter.
- Parity with today's behaviour is proven by tests; the approximation bound is stated
  (REQ-MCP-09), though the tools turned out exact.

**Non-Goals:**
- Row-level security per end user.
- Caching of results.
- Letting clients send SQL.

## Decisions

### 1. Trino `lake` catalog on the lake's JDBC catalog
Catalog properties:
- `connector.name=iceberg`
- `iceberg.catalog.type=jdbc`
- `iceberg.jdbc-catalog.driver-class=org.postgresql.Driver`
- `iceberg.jdbc-catalog.connection-url=jdbc:postgresql://timescaledb.idp-platform.svc:5432/iceberg`
- `iceberg.jdbc-catalog.connection-user` / `connection-password` from a Kubernetes
  Secret, referenced by env
- `iceberg.jdbc-catalog.catalog-name=lake`
- `iceberg.jdbc-catalog.default-warehouse-dir=s3://idp-bronze`: the default only;
  tables keep their per-layer locations
- `fs.native-s3.enabled=true`
- `s3.endpoint=http://minio.idp-platform.svc:9000`, `s3.path-style-access=true`
- S3 credentials from a Kubernetes Secret, referenced by env

The namespaces `bronze`, `silver` and `gold` appear as schemas. Trino reads the same
catalog rows and table metadata that Spark and `pyiceberg` commit, so it sees each
table's current snapshot, and access control keeps it read-only. Trino's `iceberg`
plugin must include the PostgreSQL driver; task 1.1 verifies this with a
`SHOW TABLES`.
- *Alternative:* Nessie REST, the original plan. Rejected with ADR-0028, since the
  lake no longer uses Nessie.

### 1a. A stable catalog name
The JDBC catalog addresses rows by catalog name, and Trino's `catalog-name` must
match it exactly. So:
- `catalog.name` (config, overridable by `IDP_CATALOG_NAME`) defaults to `lake`
  for any catalog that isn't a local SQLite file.
- `catalog.catalog_name(cfg)` returns it.
- Local SQLite catalogs keep `lake_<sha1(uri)[:10]>`, so the test session can hold
  several temp lakes.

The cluster's existing rows, written under the hashed name, are renamed once with
`UPDATE iceberg_tables SET catalog_name='lake'` and
`UPDATE iceberg_namespace_properties SET catalog_name='lake'` in one transaction
(`make lake-catalog-rename`, idempotent) before the new image is deployed. After
that, a password rotation doesn't change the name.

### 2. Read-only analytics identity via file-based access control
- `access-control.name=file` with rules: user `idp-mcp` gets `SELECT` on
  `lake.silver.*` and `lake.gold.*`; everything else, for every user, is denied except
  `admin`.
- Authentication in the dev cluster is header-based (`X-Trino-User`) inside the
  cluster network. That's acceptable for the prototype, and the ADR records that
  production needs TLS plus password or OIDC authentication. Access control still
  prevents writes even if a user name is spoofed as `idp-mcp`.

### 3. Query layer: `mcp/sql.py`
- `connect(cfg)` returns a `trino.dbapi` connection with `catalog=lake` and
  `user=cfg.trino.user`.
- `query(cfg, sql, params)` returns a pandas frame of the (small) result.
- **Parameters** always go through the DB-API `?` binding, which Trino executes as a
  prepared statement. The tenant, window bounds, `limit` and `sensitivity` are never
  formatted into SQL. Table and column names come only from code constants.
- **Error mapping:**
  - connection or HTTP errors become `LakeReadError(source=<trino url>)`
  - `TABLE_NOT_FOUND` becomes an empty result ("not written yet")
  - access-denied becomes `LakeReadError`, with the message kept
- **Available tenants:** `SELECT DISTINCT tenant_id FROM lake.silver."dim_subscriber$partitions"`,
  which reads partition metadata only.

### 4. Tools as SQL, with shaping in Python

| Tool | Queries |
|---|---|
| subscribers | Counts by `has_device_agreement`, `business_line`, and the roamer split (`EXISTS` against `fact_usage` with `is_roaming`). |
| usage events | `GROUP BY event_type` totals (count, `sum(gross_amount)`, `sum(qty)` by unit); `GROUP BY date_trunc(:g, started_at), event_type`, where the granularity is mapped from an allow-list (day/week/month/quarter) to a SQL literal; the period label format matches `_period`. |
| recharges | Totals, splits by `recharge_type` and `payment_method`, and the time series. |
| recharge anomalies | A CTE of successful recharges. An exact median (the mean of the middle one or two rows by `row_number()`, as pandas computes it); MAD the same way over `abs(amount - median)`; robust z computed in SQL. Amount spikes: `ORDER BY robust_z DESC LIMIT :limit`. Volume spikes from a subscriber-day aggregate with `avg`/`stddev_pop`, threshold `greatest(2, mean + k·std)`, limited. The periodic view is `GROUP BY` week or month with the anomalous subscriber-day count joined in. |
| fee charges | Totals and the product, charge-type and category-by-period breakdowns; shares computed in Python from the small grouped result. |
| fraud / credit score | The tenant's rows once per window (7, 15, 30 days back from its `max(scored_at)`), unnested to one (window, metric, value) row per value. One query gives, per window and metric, `count, avg, stddev_pop, min, max` and the two order statistics around each quantile (0.1, 0.25, 0.5, 0.75, 0.9, by `row_number()`), which Python interpolates exactly as numpy's `linear` method. A second query gives the 8-bin histogram: `np.histogram`'s bins, with the bin index corrected against the edges as numpy does, so counts are equal. The snapshot fallback applies when `scored_at` is absent or all-null. |

Every query selects `tenant_id` among its output columns, and the frame passes
through `_scoped` before shaping (REQ-FND-06). Aggregate queries return one row per
group, so the gateway cost is negligible.

### 5. Testing strategy
- **Oracle:** today's pandas implementations move to `tests/mcp_reference.py`,
  unchanged.
- **Unit tests** (no Trino) in `tests/test_mcp_tools.py`, with a fake `query()`
  returning canned frames:
  - result shaping
  - guard and error mapping
  - parameter binding (the injection case)
  - allow-list mapping for granularity and view
- **Integration** in `tests/test_mcp_trino.py`, marker `trino`:
  - `deploy/dev/trino-compose.yaml` runs Trino 480, Postgres and MinIO, with the
    same catalog file (connection URL pointed at the compose Postgres) and access
    rules as the cluster.
  - A fixture builds the lake through the real stages into that stack
    (`IDP_CATALOG_TYPE=sql`, `IDP_CATALOG_URI` set to the compose Postgres,
    `IDP_CATALOG_NAME=lake`).
  - Every tool is compared with the oracle: every field equal (to the last
    displayed digit), and rank statistics also checked against REQ-MCP-09's ±1% bound
    on the raw data.
  - `EXPLAIN` checks tenant partition pruning.
  - Writes are denied.
- **CI:** job `mcp-trino` runs `docker compose up -d --wait` and then
  `pytest -m trino`. The default job keeps `-m "not trino"`.

### 6. Local use
- `make trino-api` port-forwards Trino to `127.0.0.1:8080`.
- `make mcp-config` prints the Claude Desktop config with `IDP_TRINO_HOST=127.0.0.1`.
  `make mcp-config-minio` is retired: the MCP tools no longer need MinIO access,
  only Trino.
- `make mcp-smoke` runs every tool against the configured Trino.

## Risks / Trade-offs

- **Renaming the catalog rows under running stages.** A stage started before the
  rename would look up the old name and find no tables. Since
  `distributed-spark-runtime`, that includes the SparkApplications for Silver, Gold,
  Score, publish and maintain, started by `make` or by Airflow. → Rename during a
  quiet window: pause **all four** DAGs, and run no `make` stage targets. Rename,
  deploy the pinned-name image, check that `make lake-catalog-show` shows every row
  with `catalog_name = lake`, then resume. The rename is one transaction and
  idempotent.
- **Postgres connection headroom.** The platform TimescaleDB/Postgres allows 25
  connections. During `distributed-spark-runtime`'s verification, Airflow (about 9),
  the Spark stages' catalog connections and publish's JDBC load brought it to about
  21, and one Airflow task attempt failed near the limit. Trino's JDBC catalog adds
  its own connections to the same `iceberg` database. → Keep Trino's catalog
  connections minimal, and check `pg_stat_activity` while a Spark stage and an MCP
  session run at the same time (task 6.2). If they come close to the limit, raise
  `max_connections` in `deploy/platform/timescaledb/timescaledb.yaml`, which costs a
  little memory per connection.
- **Node memory.** Trino's coordinator is currently scaled to 0. Running it adds
  about 1.5 GB (a 1 GB heap plus the JVM) to the 12 GB minikube node, where the
  10× Spark check just fitted (12.2 of 12.3 GB with `make mk-check`). →
  `make mk-check` measures running pods, so it must pass with Trino running before
  any Spark stage runs. If it doesn't, lower Trino's heap (the queries are
  single-tenant aggregates), keep Trino scaled down while Spark stages run, or raise
  the node cap.
- **Rank statistics: exact, not approximate.** The first implementation used
  `approx_percentile`. Trino's is a T-Digest that interpolates between centroids, and
  on small or discrete data (e.g. `rule_score`'s five values) it returned values
  outside REQ-MCP-09's ±1% rank bound (p25 = 0.1647 where every value at ranks
  24–26% is 0.2). → Medians, MADs and percentiles are exact, from window-function
  order statistics. They still run in Trino over the tenant's partitions; the cost
  is a sort per window and metric (or over the tenant's successful recharges), which
  is small at per-tenant sizes. REQ-MCP-09 still permits approximation, so an
  approximate path can come back if a tenant's data outgrows the sort.
- **Float summation order.** Trino and pandas add doubles in different orders, so a
  sum exactly on a rounding boundary (Silver's 5-decimal `gross_amount`, shown to 4)
  can round either way. → The integration test compares to the last displayed
  digit.
- **Trino coordinator-only with a 1 G heap on minikube.** → The queries are
  aggregates over one tenant's partitions. Memory limits are configured, and the
  `EXPLAIN` test keeps pruning honest.
- **Header-based auth in dev.** → Access control still enforces read-only. TLS plus
  authentication for production is recorded in the ADR.
- **The official MinIO images are no longer publicly pullable** (Docker Hub and
  quay.io refuse anonymous pulls, like `minio/mc` before it). → The compose stack uses
  `bitnamilegacy/minio:2025.4.22`, Bitnami's frozen build of the cluster's MinIO commit
  (2025-04-22T22-12-26Z), which is public, so the same stack runs locally and in CI.
- **Test infrastructure weight:** a Docker stack for integration. → Unit tests cover
  the logic without Docker. Only `-m trino` needs the stack.
- **Behavioural drift from the oracle over time.** → The oracle is frozen. Changing a
  tool's semantics means changing both, on purpose.

## Migration Plan

1. After `iceberg-incremental-lakehouse` is deployed: rename the catalog rows to
   `lake` (`make lake-catalog-rename`, with all DAGs paused and no `make` stage
   targets running), deploy the image that pins the name
   (`make image app-deploy airflow-reload`), then `helm upgrade trino` with the new
   values (catalog plus access control), then `make trino-api`.
2. Verify with `SHOW TABLES FROM lake.silver` and `make mcp-smoke`.
3. Re-run `make mcp-config` and update `claude_desktop_config.json`.
4. Rollback: the previous image's tools read the lake through `pyiceberg`. Point
   `claude_desktop_config.json` back at them.
