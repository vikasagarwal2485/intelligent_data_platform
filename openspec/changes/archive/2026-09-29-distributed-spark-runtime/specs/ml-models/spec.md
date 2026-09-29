# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-ML-07 — Scoring is a separate inference stage
Gold SHALL build **feature tables only**. A separate **Score** stage
(`idp score`) SHALL read the Gold features and the trained models and write
`credit_scores`/`fraud_scores` back to Gold. Scoring SHALL be distributed: the
models are shipped to the workers, and each partition of the feature table is
scored independently, so no process ever holds the whole feature table. When a model
is absent, the Score stage SHALL skip that score table (graceful) rather than fail.
Every row a score run writes SHALL carry the same `scored_at` UTC timestamp, and
successive runs SHALL accumulate as score history (consumed by REQ-MCP-07/08).

#### Scenario: SCN-ML-07.1 — Gold is features-only
- **WHEN** `to_gold` runs
- **THEN** its output is `credit_features`/`fraud_features` and contains no score tables

#### Scenario: SCN-ML-07.2 — Score writes score tables
- **WHEN** the Score stage runs with models available
- **THEN** it writes `credit_scores` and `fraud_scores` from the Gold features

#### Scenario: SCN-ML-07.3 — Missing model skipped
- **WHEN** the Score stage runs and no fraud model exists in the store
- **THEN** it writes `credit_scores` only and reports no error

#### Scenario: SCN-ML-07.4 — Runs are timestamped
- **WHEN** the Score stage runs twice on different days
- **THEN** each run's rows carry that run's single `scored_at`, and rows from both
  runs are readable from Gold

#### Scenario: SCN-ML-07.5 — Distributed scores equal in-process scores
- **WHEN** the same Gold features are scored by the distributed Score stage and by
  calling the models directly on the whole table in one process
- **THEN** every `(tenant_id, service_id)` gets identical `pd`, `recommended_limit`,
  `fraud_proba`, `anomaly_score`, `rule_score` and `fraud_score` values

## ADDED Requirements

### Requirement: REQ-ML-09 — Bounded training set
The `train` stage SHALL fit on at most `ml.train_max_rows` labelled rows. When more
labelled rows exist, it SHALL take a stratified random sample per tenant and per
label class with a fixed seed, so each tenant's classes keep their proportions.
Training memory SHALL therefore not grow with the subscriber base. The sample size
and seed SHALL be recorded with the training metrics.

#### Scenario: SCN-ML-09.1 — Sample cap applied
- **WHEN** 100 000 labelled rows exist and `ml.train_max_rows` is 20 000
- **THEN** training uses 20 000 rows, each tenant's default and fraud rates in the
  sample are within 1 percentage point of the full set, and the metrics report
  `sampled_rows = 20000`

#### Scenario: SCN-ML-09.2 — Small sets unchanged
- **WHEN** fewer labelled rows exist than the cap
- **THEN** training uses all of them, exactly as before
