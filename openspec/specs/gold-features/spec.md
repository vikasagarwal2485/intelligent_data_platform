# Gold Features Specification

## Purpose

Produce analytics-ready, per-subscriber feature tables from Silver. Gold is a
standalone stage that reads Silver and writes Gold feature tables only; scoring
is the separate Score stage (REQ-ML-07).

## Requirements

### Requirement: REQ-GLD-01 — Credit and fraud feature tables
The system SHALL build per-`(tenant_id, service_id)` feature tables:
`credit_features` (recharge ability/reliability, spend, tenure) and
`fraud_features` (velocity, traffic asymmetry, roaming, recharge anomalies),
computed over configurable lookback windows (e.g. 7/30/90 days).

#### Scenario: SCN-GLD-01.1 — Feature tables produced
- **WHEN** `to_gold` runs on valid Silver
- **THEN** `credit_features` and `fraud_features` are produced, one row per
  subscriber, with no nulls in the model feature columns

#### Scenario: SCN-GLD-01.2 — Bounded ratios
- **WHEN** fraud features are built
- **THEN** `originating_ratio` and `roaming_share` are within [0, 1]

### Requirement: REQ-GLD-02 — Tokenized join integrity
The feature aggregation SHALL join facts back to `dim_subscriber` via the
tenant-tokenized `service_id`, without exposing raw identifiers.

#### Scenario: SCN-GLD-02.1 — Join coverage
- **WHEN** features are built
- **THEN** the vast majority of fact subjects map to a subscriber in the dimension

### Requirement: REQ-GLD-04 — Idempotent Gold feature writes
Persisting `credit_features` and `fraud_features` SHALL replace the partitions of
the tenants present in the new output. Each feature table SHALL then contain exactly
one row per `(tenant_id, service_id)`, however many times the Gold stage has run.
Partitions of other tenants SHALL be left untouched.

#### Scenario: SCN-GLD-04.1 — Hourly refresh keeps one row per subscriber
- **WHEN** the Gold stage runs repeatedly against the same lake (as the hourly
  features DAG does)
- **THEN** reading `credit_features` back yields exactly one row per
  `(tenant_id, service_id)`

#### Scenario: SCN-GLD-04.2 — Refresh reflects new Silver
- **WHEN** Silver changes between two Gold runs
- **THEN** after the second run the Gold feature values reflect only the new Silver,
  with no rows left over from the first run

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
