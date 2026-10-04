# Release notes

Changes are recorded here for maintainers and operators. Add work under
**Unreleased**; move it to a dated version section when an actual release is made.
Package version `0.1.0` is currently a development baseline. No release tag was
present when this history was prepared on 2026-10-04.

## Unreleased

### Repository and development

- Added root `AGENTS.md` and tracked `internal/` operator memory with a repository
  map, decision reminders, runbook and dated handoff.
- Added Python 3.11 and editor defaults, overridable workspace interpreter/venv
  settings and `make check` for dependency health, Python/config/shell syntax,
  synchronized package metadata and onboarding links.
- Make targets select the same Python for Spark drivers and workers. Constrained
  the MCP dependency to v1, matching the server's existing `FastMCP` API.
- Excluded local environment files, workstation caches and operator memory from
  the relevant Git/Docker contexts. Corrected onboarding documentation to the
  current OpenSpec layout and Spark runtime.

### Continuous integration

- Expanded the existing GitHub Actions workflow with workspace checks and a real
  PostgreSQL service for the Spark JDBC publish test.
- Kept real Trino integration coverage and made unavailable/misconfigured Trino
  fail in CI instead of skipping. Added a bounded Compose health wait, service logs
  on failure and unconditional disposable-stack cleanup.
- Updated the Bronze access-denial integration check to target the current raw
  `tmf_party` table, so it tests permissions on an existing table.
- Added manual dispatch, dependency caching, job timeouts, superseded-run
  cancellation and explicit read-only repository permissions.

## 0.1.0 development baseline — inspected 2026-10-04

Based on commit `0be5efe`, package metadata, current code and the
[accepted decision log](docs/decisions/decision-log.md). This section summarizes
the existing implementation; it is not a claim of a tagged or published release.

### Platform capabilities

- Multi-tenant TM Forum landing/Kafka ingestion into raw, append-only Bronze
  Iceberg tables. Silver validates and conforms events, resolves current reference
  entities, tokenizes PII and quarantines invalid records.
- Incremental Silver facts and Spark Gold feature tables for consenting subscribers,
  backed by a shared SQL/JDBC Iceberg catalog. SQLite supports local runs;
  PostgreSQL/TimescaleDB and MinIO/S3 support the cluster.
- Credit probability-of-default and affordability sizing, plus fraud rules,
  gradient boosting and anomaly detection. Labels stay separate from the lake;
  training uses a bounded stratified sample.
- MLflow versioned models with quality-gated `champion` promotion, with an
  object-store fallback when no registry is configured. Spark scores distributed
  partitions and appends timestamped Gold history.
- Atomic serving-table publication into PostgreSQL/TimescaleDB, FastAPI scoring
  and Keycloak JWT tenant authentication in cluster overlays.
- Seven read-only MCP analytics tools query tenant-scoped Silver and Gold through
  Trino, with bounded results and integration parity checks.
- Separate platform and application deployment ownership, generated SparkApplication
  profiles, and four Airflow DAGs for features, training, inference and maintenance.

### Recent changes and compatibility

- Raw Bronze now preserves TMF payloads and envelope/Kafka metadata; conformance
  belongs in Silver (ADR-0032). The old `idp migrate-lake` path is removed. Legacy
  lakes require an explicitly planned reset and re-stream, followed by rebuilding
  downstream tables; review the affected environment before resetting it.
- `customer_tenure_days` replaces `account_tenure_days` in Gold, the credit model
  and API feature contract. Retrain old models after migrating the feature schema;
  the API rejects unknown feature names with HTTP 422.
- All Airflow tasks now submit SparkApplications (ADR-0035). Labels are seeded
  separately with `make labels`; training requires the labels store to exist.
- Spark shuffle partitions follow the stage profile; fact conformance runs in
  parallel and Gold reuses shared aggregates (ADR-0036). Existing checkpoints need
  one authorized `make silver-full-refresh` to adopt a new partition count.
- Customer Kafka topics use delete policy with seven-day retention. `make topics`
  applies the configuration to existing topics without requiring a purge.
- On-demand fraud scoring uses the model's stored training anomaly range
  (ADR-0037). Retrain older models without that range; batch scoring continues to
  normalize over the scored population and can differ when populations differ.

See [internal/runbook.md](internal/runbook.md) for development checks and operator
reminders, and [README.md](README.md) for the deployment sequence.
