# Spec Delta

## MODIFIED Requirements

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
