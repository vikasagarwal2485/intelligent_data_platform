# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-ML-03 — Training is a separate, offline stage
The `train` stage SHALL run separately from the ELT and offline. It SHALL fit both
models on labelled Gold features (REQ-ML-06) with an out-of-sample split, report
hold-out metrics (credit: AUC/KS/Gini; fraud: PR-AUC and precision@k), and persist
the models to the shared model store (REQ-ML-04). A local in-memory path, which
builds features from generated Silver and uses the generator's labels, SHALL remain
available for cluster-free dev and tests only.

#### Scenario: SCN-ML-03.1 — Models persisted
- **WHEN** `train` runs with `persist=True`
- **THEN** `credit_model` and `fraud_model` artifacts are written and reload to
  produce identical predictions

#### Scenario: SCN-ML-03.2 — Adverse-action reason codes
- **WHEN** a credit decision is produced
- **THEN** `reason_codes()` returns the top-k features raising the PD (adverse
  reasons), via SHAP when the optional `shap` package is installed and otherwise
  via the interpretable logistic scorecard, and the scoring API includes them in
  the `/credit-limit` response

#### Scenario: SCN-ML-03.3 — Train job does not read Silver
- **WHEN** `idp train` runs with no in-memory dataset
- **THEN** it reads only the Gold feature tables and the labels store, and never
  builds features from Silver or regenerates synthetic events

### Requirement: REQ-ML-06 — Train on historical Gold + a labels store
Training SHALL learn from the lake, not by regenerating synthetic data. Ground-truth
labels are persisted to a **labels/outcomes store** (`s3://<prefix>-ml/labels`),
tokenized on `service_id` and kept OUTSIDE the Gold/serving path (no leakage). The
`train` stage SHALL read historical **Gold features + the labels store**, join them,
fit, and persist the models. It SHALL fail clearly if either input is missing, or if
the join produces no rows. When the store holds more than one label for a
subscriber, training SHALL use exactly one of them, so the join never fans out
into duplicate training rows. Which stored label is used is not guaranteed; a
single current label per subscriber is the labels store's responsibility
(REQ-ML-08). In this prototype the labels are
regenerated from the synthetic generator, so they SHALL be seeded with the same
synthetic parameters (subscribers, days, seed) as the data that produced Gold.
(A local in-memory path is retained for cluster-free dev/tests.)

#### Scenario: SCN-ML-06.1 — Fit from the lake, no regeneration
- **WHEN** Gold features exist and the labels store is seeded, and `train` runs with
  no in-memory dataset
- **THEN** it reads both from storage, fits, and writes the models to the store

#### Scenario: SCN-ML-06.2 — Clear error without inputs
- **WHEN** `train` runs on the lake path but Gold features or labels are missing
- **THEN** it raises a clear error naming the missing input (Gold / labels)

#### Scenario: SCN-ML-06.3 — Labels stay out of serving
- **WHEN** labels are written
- **THEN** they go to the labels store (not the Gold feature tables) and `publish`
  never copies them to the serving DB

#### Scenario: SCN-ML-06.4 — Mismatched labels fail clearly
- **WHEN** the labels were seeded with different synthetic parameters than the
  Gold features, so no tokenized `service_id` joins
- **THEN** `train` raises an error explaining the empty join and that the labels
  must be re-seeded with matching parameters

#### Scenario: SCN-ML-06.5 — Duplicate labels do not fan out
- **WHEN** the labels store holds two labels for the same `(tenant_id, service_id)`
- **THEN** the labels read for training contain exactly one row for that subscriber,
  and the training set has one row per subscriber

### Requirement: REQ-ML-07 — Scoring is a separate inference stage
Gold SHALL build **feature tables only**. A separate **Score** stage
(`idp score`) SHALL read Gold features + the trained models and write
`credit_scores`/`fraud_scores` back to Gold. When a model is absent the Score
stage SHALL skip that score table (graceful) rather than fail. Every row a score run
writes SHALL carry the same `scored_at` UTC timestamp, and successive runs SHALL
accumulate as score history (consumed by REQ-MCP-07/08).

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
