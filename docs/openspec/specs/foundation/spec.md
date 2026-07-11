# Foundation Specification

## Purpose

Cross-cutting primitives every other capability depends on: configuration,
multi-tenancy isolation, GDPR PII handling, and lake storage abstraction.

## Requirements

### Requirement: Configuration with environment overrides
The system SHALL load platform configuration from `config/platform.yaml` into a
typed object, and SHALL allow environment variables (`IDP_*`) to override select
values (Kafka bootstrap, S3 endpoint/credentials/bucket prefix, synthetic volume).

#### Scenario: Load config
- **WHEN** `load_config()` is called with no argument
- **THEN** it returns a config exposing `tenants`, `storage`, `kafka`, `s3`,
  `synthetic`, `features`, `credit`, and `fraud`

#### Scenario: S3 mode via env
- **WHEN** the environment sets `IDP_S3_ENDPOINT`
- **THEN** `cfg.s3["enabled"]` is `True` and the endpoint is used for lake I/O

### Requirement: Tenant stamping and isolation
The system SHALL treat `tenant_id` as the mandatory, leading isolation column on
every dataset, and SHALL provide a single gateway to read tenant-scoped data such
that one tenant can never read another tenant's rows.

#### Scenario: Scope to a tenant
- **WHEN** `scope_to_tenant(df, "tenant_a")` is called on a frame containing
  multiple tenants
- **THEN** only `tenant_a` rows are returned and no `tenant_b` values are present

#### Scenario: Reject untagged data
- **WHEN** a frame lacking a `tenant_id` column is passed to a tenant guard
- **THEN** the system raises a `TenantIsolationError`

### Requirement: GDPR PII tokenization
The system SHALL tokenize PII columns (MSISDN/IMSI/IMEI/phone/email/other-party)
using a per-tenant salted SHA-256, deterministically, so that tokens are stable
join keys within a tenant and differ across tenants, and SHALL never persist raw
identifiers beyond Bronze.

#### Scenario: Deterministic, tenant-specific tokens
- **WHEN** the same raw value is tokenized twice with the same tenant salt
- **THEN** the tokens are equal; **AND WHEN** tokenized with a different tenant's
  salt **THEN** the tokens differ

#### Scenario: Consent gating
- **WHEN** a subscriber has not granted analytics consent
- **THEN** `filter_by_consent(purpose="analytics")` excludes that subscriber

### Requirement: Lake storage abstraction (local or S3)
The system SHALL read/write Parquet to either local directories or S3/MinIO based
on config, mapping each medallion layer to its own bucket `<prefix>-<layer>`.

#### Scenario: Per-layer bucket resolution
- **WHEN** S3 is enabled with prefix `idp`
- **THEN** `lake_uri("silver")` returns `s3://idp-silver`

#### Scenario: Local fallback
- **WHEN** S3 is disabled
- **THEN** `lake_uri("bronze")` returns the configured local bronze path
