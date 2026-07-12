# Gold Features Specification

## Purpose

Produce analytics-ready, per-subscriber feature tables (and optional scores) from
Silver. Gold is a standalone stage that reads Silver and writes Gold.

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

### Requirement: REQ-GLD-03 — Optional scoring
The Gold stage SHALL append `credit_scores`/`fraud_scores` when trained models
are provided or exist in the artifact store, and SHALL otherwise produce feature
tables only (the lake carries no labels to train from).

#### Scenario: SCN-GLD-03.1 — Features-only without models
- **WHEN** `to_gold` runs and no trained models are available
- **THEN** Gold contains `credit_features` and `fraud_features` and no score tables

#### Scenario: SCN-GLD-03.2 — Scores with models
- **WHEN** `to_gold` runs with trained models supplied
- **THEN** Gold additionally contains `credit_scores` and `fraud_scores`
