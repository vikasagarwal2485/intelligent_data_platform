# Foundation delta

## MODIFIED Requirements

### Requirement: REQ-FND-01 — Configuration with environment overrides
The system SHALL load platform configuration from `config/platform.yaml` into a
typed object, and SHALL allow environment variables to override select values:
- Kafka: `IDP_KAFKA_BOOTSTRAP`, `IDP_SCHEMA_REGISTRY`
- object storage: `IDP_S3_ENDPOINT`, `IDP_S3_ACCESS_KEY`, `IDP_S3_SECRET_KEY`,
  `IDP_S3_BUCKET_PREFIX`
- the lake's Iceberg catalog: `IDP_CATALOG_TYPE`, `IDP_CATALOG_URI`,
  `IDP_CATALOG_NAME`
- the SQL query engine used by the analytics tools: `IDP_TRINO_HOST`,
  `IDP_TRINO_PORT`, `IDP_TRINO_CATALOG`, `IDP_TRINO_CA_CERT` (the certificate
  that verifies the engine's HTTPS endpoint)
- the analytics tools' identity: `IDP_MCP_TOKEN_URL`, `IDP_MCP_CLIENT_ID`,
  `IDP_MCP_CLIENT_SECRET` (setting the client id enables it)
- the TimescaleDB serving store: `IDP_TS_HOST`, `IDP_TS_PORT`, `IDP_TS_DB`,
  `IDP_TS_USER`, `IDP_TS_PASSWORD`
- the Spark runtime: `IDP_SPARK_MASTER`, `IDP_SPARK_DRIVER_MEMORY`
- synthetic volume: `IDP_SUBSCRIBERS`, `IDP_DAYS`, `IDP_FRAUD_RATE`

#### Scenario: SCN-FND-01.1 — Load config
- **WHEN** `load_config()` is called with no argument
- **THEN** it returns a config exposing `tenants`, `storage`, `kafka`, `s3`,
  `catalog`, `lake`, `trino`, `mcp`, `timescale`, `spark`, `synthetic`, `features`,
  `credit`, and `fraud`

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

#### Scenario: SCN-FND-01.6 — Query engine via env
- **WHEN** the environment sets `IDP_TRINO_HOST` and `IDP_TRINO_PORT`
- **THEN** `cfg.trino` carries them, and the analytics tools connect to that
  endpoint over HTTPS, to the configured catalog, as the identity the `mcp` client
  credentials obtain (there is no configured Trino user)

#### Scenario: SCN-FND-01.7 — Stable catalog name
- **WHEN** the catalog is not a local SQLite file and no name is configured
- **THEN** `cfg.catalog["name"]` is `lake`, independent of the connection URI and
  its credentials, so Spark, `pyiceberg` and Trino address the same catalog rows;
  `IDP_CATALOG_NAME` overrides it, and a local SQLite catalog keeps a per-file name
  so one process can use several local lakes
