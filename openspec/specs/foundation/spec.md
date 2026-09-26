# Foundation Specification

## Purpose

Cross-cutting primitives every other capability depends on: configuration,
multi-tenancy isolation, GDPR PII handling, and lake storage abstraction.

## Requirements

### Requirement: REQ-FND-01 — Configuration with environment overrides
The system SHALL load platform configuration from `config/platform.yaml` into a
typed object, and SHALL allow environment variables to override select values:
Kafka (`IDP_KAFKA_BOOTSTRAP`, `IDP_SCHEMA_REGISTRY`), object storage
(`IDP_S3_ENDPOINT`, `IDP_S3_ACCESS_KEY`, `IDP_S3_SECRET_KEY`,
`IDP_S3_BUCKET_PREFIX`), the TimescaleDB serving store (`IDP_TS_HOST`,
`IDP_TS_PORT`, `IDP_TS_DB`, `IDP_TS_USER`, `IDP_TS_PASSWORD`), the Spark runtime
(`IDP_SPARK_MASTER`, `IDP_SPARK_DRIVER_MEMORY`) and synthetic volume
(`IDP_SUBSCRIBERS`, `IDP_DAYS`, `IDP_FRAUD_RATE`).

#### Scenario: SCN-FND-01.1 — Load config
- **WHEN** `load_config()` is called with no argument
- **THEN** it returns a config exposing `tenants`, `storage`, `kafka`, `s3`,
  `timescale`, `spark`, `synthetic`, `features`, `credit`, and `fraud`

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
The system SHALL read and write the lake in either local directories or S3/MinIO,
based on config, mapping each medallion layer to its own bucket `<prefix>-<layer>`.
The same layer SHALL resolve to the same location for Spark (`s3a://` scheme) and
for pandas/ML readers (`s3://` scheme), so both see the same data.

#### Scenario: SCN-FND-04.1 — Per-layer bucket resolution
- **WHEN** S3 is enabled with prefix `idp`
- **THEN** `lake_uri("silver")` returns `s3://idp-silver`

#### Scenario: SCN-FND-04.2 — Local fallback
- **WHEN** S3 is disabled
- **THEN** `lake_uri("bronze")` returns the configured local bronze path

#### Scenario: SCN-FND-04.3 — Spark resolves the same location
- **WHEN** S3 is enabled with prefix `idp`
- **THEN** the Spark location of the Silver layer is `s3a://idp-silver`; **AND
  WHEN** S3 is disabled **THEN** it is the same local path pandas uses
