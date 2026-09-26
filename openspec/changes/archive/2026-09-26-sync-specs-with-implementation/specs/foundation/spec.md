# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-FND-01 — Configuration with environment overrides
The system SHALL load platform configuration from `config/platform.yaml` into a
typed object, and SHALL allow environment variables to override select values:
Kafka (`IDP_KAFKA_BOOTSTRAP`, `IDP_SCHEMA_REGISTRY`), object storage
(`IDP_S3_ENDPOINT`, `IDP_S3_ACCESS_KEY`, `IDP_S3_SECRET_KEY`,
`IDP_S3_BUCKET_PREFIX`), the TimescaleDB serving store (`IDP_TS_HOST`,
`IDP_TS_PORT`, `IDP_TS_DB`, `IDP_TS_USER`, `IDP_TS_PASSWORD`) and synthetic volume
(`IDP_SUBSCRIBERS`, `IDP_DAYS`, `IDP_FRAUD_RATE`).

#### Scenario: SCN-FND-01.1 — Load config
- **WHEN** `load_config()` is called with no argument
- **THEN** it returns a config exposing `tenants`, `storage`, `kafka`, `s3`,
  `timescale`, `synthetic`, `features`, `credit`, and `fraud`

#### Scenario: SCN-FND-01.2 — S3 mode via env
- **WHEN** the environment sets `IDP_S3_ENDPOINT`
- **THEN** `cfg.s3["enabled"]` is `True` and the endpoint is used for lake I/O

#### Scenario: SCN-FND-01.3 — Serving store enabled via env
- **WHEN** the environment sets `IDP_TS_HOST`
- **THEN** `cfg.timescale["enabled"]` is `True` and that host is used by the
  publish stage and the serving lookup
