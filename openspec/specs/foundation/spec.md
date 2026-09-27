# Foundation Specification

## Purpose

Cross-cutting primitives every other capability depends on: configuration,
multi-tenancy isolation, GDPR PII handling, and lake storage abstraction.

## Requirements

### Requirement: REQ-FND-01 — Configuration with environment overrides
The system SHALL load platform configuration from `config/platform.yaml` into a
typed object, and SHALL allow environment variables to override select values:
- Kafka: `IDP_KAFKA_BOOTSTRAP`, `IDP_SCHEMA_REGISTRY`
- object storage: `IDP_S3_ENDPOINT`, `IDP_S3_ACCESS_KEY`, `IDP_S3_SECRET_KEY`,
  `IDP_S3_BUCKET_PREFIX`
- the lake's Iceberg catalog: `IDP_CATALOG_TYPE`, `IDP_CATALOG_URI`
- the TimescaleDB serving store: `IDP_TS_HOST`, `IDP_TS_PORT`, `IDP_TS_DB`,
  `IDP_TS_USER`, `IDP_TS_PASSWORD`
- the Spark runtime: `IDP_SPARK_MASTER`, `IDP_SPARK_DRIVER_MEMORY`
- synthetic volume: `IDP_SUBSCRIBERS`, `IDP_DAYS`, `IDP_FRAUD_RATE`

#### Scenario: SCN-FND-01.1 — Load config
- **WHEN** `load_config()` is called with no argument
- **THEN** it returns a config exposing `tenants`, `storage`, `kafka`, `s3`,
  `catalog`, `timescale`, `spark`, `synthetic`, `features`, `credit`, and `fraud`

#### Scenario: SCN-FND-01.2 — S3 mode via env
- **WHEN** the environment sets `IDP_S3_ENDPOINT`
- **THEN** `cfg.s3["enabled"]` is `True` and the endpoint is used for lake I/O

#### Scenario: SCN-FND-01.3 — Serving store enabled via env
- **WHEN** the environment sets `IDP_TS_HOST`
- **THEN** `cfg.timescale["enabled"]` is `True` and that host is used by the
  publish stage and the serving lookup

#### Scenario: SCN-FND-01.4 — Spark runtime via env
- **WHEN** the environment sets `IDP_SPARK_MASTER` and `IDP_SPARK_DRIVER_MEMORY`
- **THEN** `cfg.spark` carries those values, and they override the defaults
  (`local[*]` and the configured driver memory)

#### Scenario: SCN-FND-01.5 — Catalog via env
- **WHEN** the environment sets `IDP_CATALOG_TYPE=sql` and `IDP_CATALOG_URI` to the
  platform Postgres catalog database
- **THEN** `cfg.catalog` carries them, and every lake reader and writer uses that
  catalog; **AND WHEN** neither is set **THEN** the local file-based (SQLite)
  catalog under the local lake root is used

### Requirement: REQ-FND-02 — Tenant stamping and isolation
The system SHALL treat `tenant_id` as the mandatory, leading isolation column on
every dataset, and SHALL provide a single gateway to read tenant-scoped data such
that one tenant can never read another tenant's rows.

#### Scenario: SCN-FND-02.1 — Scope to a tenant
- **WHEN** `scope_to_tenant(df, "tenant_a")` is called on a frame containing
  multiple tenants
- **THEN** only `tenant_a` rows are returned and no `tenant_b` values are present

#### Scenario: SCN-FND-02.2 — Reject untagged data
- **WHEN** a frame lacking a `tenant_id` column is passed to a tenant guard
- **THEN** the system raises a `TenantIsolationError`

### Requirement: REQ-FND-03 — GDPR PII tokenization
The system SHALL tokenize PII columns (MSISDN/IMSI/IMEI/phone/email/other-party)
using a per-tenant salted SHA-256, deterministically, so that tokens are stable
join keys within a tenant and differ across tenants, and SHALL never persist raw
identifiers beyond Bronze. Tokens computed in the Spark transforms and in Python
(e.g. for the labels store) SHALL be identical for the same tenant and value.

#### Scenario: SCN-FND-03.1 — Deterministic, tenant-specific tokens
- **WHEN** the same raw value is tokenized twice with the same tenant salt
- **THEN** the tokens are equal; **AND WHEN** tokenized with a different tenant's
  salt **THEN** the tokens differ

#### Scenario: SCN-FND-03.2 — Consent gating
- **WHEN** a subscriber has not granted analytics consent
- **THEN** `filter_by_consent(purpose="analytics")` excludes that subscriber

#### Scenario: SCN-FND-03.3 — Spark and Python tokens agree
- **WHEN** a subscriber's MSISDN is tokenized by the Spark Silver transform and by
  the Python tokenizer with the same tenant salt
- **THEN** the two tokens are identical, so labels join Gold features

### Requirement: REQ-FND-04 — Lake storage abstraction (local or S3)
The system SHALL keep the lake's Bronze, Silver and Gold as Iceberg tables in
namespaces `bronze`, `silver` and `gold`, resolved through one configured catalog.
It SHALL use an Iceberg JDBC catalog: on the platform Postgres in the cluster, and
a local file-based (SQLite) catalog otherwise. Both SHALL keep every table's full
snapshot history. Each table's data SHALL live in its layer's
location: the bucket `<prefix>-<layer>` when S3 is enabled, or the configured local
layer path otherwise. Spark and the Python readers and writers (landing, ML,
serving, analytics) SHALL resolve a table through the same catalog, so both see the
same committed snapshots. Python readers SHALL push row filters (e.g. `tenant_id`)
down to the table scan instead of reading whole tables.

#### Scenario: SCN-FND-04.1 — Per-layer bucket resolution
- **WHEN** S3 is enabled with prefix `idp`
- **THEN** `lake_uri("silver")` returns `s3://idp-silver`, and Silver tables store
  their data under it

#### Scenario: SCN-FND-04.2 — Local fallback
- **WHEN** S3 is disabled
- **THEN** `lake_uri("bronze")` returns the configured local bronze path, and Bronze
  tables store their data under it

#### Scenario: SCN-FND-04.3 — Spark resolves the same location
- **WHEN** a table is written by Spark and then read by a Python reader, or the
  reverse
- **THEN** both resolve it through the same catalog and see the same committed rows

#### Scenario: SCN-FND-04.4 — Filtered reads
- **WHEN** a Python reader asks for one tenant's rows of a Gold or Silver table
- **THEN** the filter is applied in the table scan, and files holding only other
  tenants' partitions are not read

### Requirement: REQ-FND-05 — Analytics consent enforced on the ML path
The Gold feature tables SHALL contain only subscribers whose `consent_analytics` is
true. Because model training, the Score stage, the serving store and the score
analytics all read Gold, a non-consenting subscriber SHALL never be trained on,
scored, published or reported per subscriber. A null or missing consent value SHALL
be treated as no consent. If the subscriber data has no consent column at all, the
Gold stage SHALL fail with a clear error rather than include every subscriber.

#### Scenario: SCN-FND-05.1 — Non-consenting subscriber excluded from features
- **WHEN** Gold is built from Silver in which subscriber S has
  `consent_analytics = false`
- **THEN** S is absent from `credit_features` and `fraud_features`, and every
  consenting subscriber is present exactly once

#### Scenario: SCN-FND-05.2 — Excluded downstream
- **WHEN** training, Score and publish run after that Gold build
- **THEN** S appears in no training set, in neither score table, and in no
  `gold.*` serving table

#### Scenario: SCN-FND-05.3 — Null consent fails closed
- **WHEN** subscriber S has a null `consent_analytics`
- **THEN** S is excluded exactly as if consent were false

#### Scenario: SCN-FND-05.4 — Missing consent column is an error
- **WHEN** the Gold stage runs on a `dim_subscriber` that has no
  `consent_analytics` column
- **THEN** it fails with an error naming the missing consent column and writes no
  Gold tables

#### Scenario: SCN-FND-05.5 — Revocation takes effect on refresh
- **WHEN** S revokes consent, then Gold is refreshed and Score and publish run
- **THEN** S no longer appears in the Gold feature tables, the latest score run, or
  the serving tables

### Requirement: REQ-FND-06 — Tenant-scoped reads go through the gateway
Every tenant-scoped read that serves a caller's request SHALL obtain its rows
through the tenant gateway (`scope_to_tenant`). This covers the MCP analytics
tools; the serving lookup filters by tenant in its query. Data lacking the
`tenant_id` column SHALL be rejected as a tenant-isolation violation. It SHALL NOT be
treated as empty, and it SHALL NOT be returned unscoped.

#### Scenario: SCN-FND-06.1 — Untagged table is an isolation error
- **WHEN** an MCP tool reads a lake table that has no `tenant_id` column
- **THEN** the tool returns a structured error identifying a tenant-isolation
  violation and the table, and returns no rows

#### Scenario: SCN-FND-06.2 — Gateway scopes every tool
- **WHEN** any MCP tool is called for `tenant_a` on a lake holding `tenant_a` and
  `tenant_b`
- **THEN** the rows it aggregates are exactly those that `scope_to_tenant` returns
  for `tenant_a`
