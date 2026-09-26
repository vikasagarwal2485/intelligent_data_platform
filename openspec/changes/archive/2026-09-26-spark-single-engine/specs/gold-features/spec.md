# Spec Delta

## ADDED Requirements

### Requirement: REQ-GLD-05 — Gold reads Silver from the lake
The Gold stage SHALL always read the Silver tables from the lake: object storage
when configured, otherwise the local lake directory. It SHALL NOT accept in-memory
input. It SHALL use the lookback windows from configuration (`features.windows`),
and SHALL fail clearly when `dim_subscriber` is absent or empty.

#### Scenario: SCN-GLD-05.1 — Gold built from the persisted Silver
- **WHEN** Silver has been written and the Gold stage runs as a separate process
- **THEN** it builds `credit_features` and `fraud_features` from that Silver, one
  row per `(tenant_id, service_id)`

#### Scenario: SCN-GLD-05.2 — Missing Silver
- **WHEN** the Gold stage runs and `dim_subscriber` is absent or empty
- **THEN** it fails with an error instructing to run the Silver stage first, and
  writes no Gold tables
