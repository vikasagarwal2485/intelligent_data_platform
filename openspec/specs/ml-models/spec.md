# ML Models Specification

## Purpose

Alternative credit-limit scoring and fraud detection, plus offline training that
is separate from the ELT (labels never enter the lake — ADR-0014).

## Requirements

### Requirement: REQ-ML-01 — Credit-limit model (PD + affordability sizing)
The system SHALL train a probability-of-default classifier (gradient-boosted;
`HistGradientBoostingClassifier` standing in for LightGBM/XGBoost) with an
interpretable logistic-scorecard baseline, and SHALL size a recommended limit as
`affordability(recharge, tenure) × (1 − PD)` clipped to policy bands.

#### Scenario: SCN-ML-01.1 — PD is a probability
- **WHEN** the credit model scores a feature frame
- **THEN** every predicted PD is within [0, 1]

#### Scenario: SCN-ML-01.2 — Higher risk, not-higher limit
- **WHEN** `recommend_limit` is applied
- **THEN** the mean recommended limit for high-PD subscribers is ≤ that of low-PD
  subscribers, and every limit is within the configured [min, max] band

#### Scenario: SCN-ML-01.3 — Out-of-sample evaluation
- **WHEN** the credit model is evaluated on a holdout split
- **THEN** it reports AUC, KS, and Gini, with AUC clearly above 0.5

### Requirement: REQ-ML-02 — Fraud model (layered, fused score)
The system SHALL combine deterministic rules, a gradient-boosted classifier
(imbalance-weighted, optimized for PR-AUC), and an IsolationForest anomaly score
into a single fused fraud score in [0, 1].

#### Scenario: SCN-ML-02.1 — Rules flag SIM-box
- **WHEN** a subscriber exceeds the originating-ratio and call-velocity thresholds
- **THEN** the SIM-box rule flag is set and contributes to the fused score

#### Scenario: SCN-ML-02.2 — Fraud ranked above normal
- **WHEN** the fraud model scores a labelled frame
- **THEN** the mean fraud score for fraud subscribers exceeds that for normal ones,
  and out-of-sample PR-AUC exceeds the base rate

### Requirement: REQ-ML-03 — Training is a separate, offline stage
The `train` stage SHALL run separately from the ELT and offline. It SHALL fit both
models on labelled Gold features (REQ-ML-06) with an out-of-sample split, report
hold-out metrics (credit: AUC/KS/Gini; fraud: PR-AUC and precision@k), and persist
the models to the shared model store (REQ-ML-04). Training SHALL always read Gold
features and labels from the lake. For cluster-free dev and tests, that is a local
lake built by the same stages; there SHALL be no path that builds features in
memory for training.

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
- **WHEN** `idp train` runs
- **THEN** it reads only the Gold feature tables and the labels store, and never
  builds features from Silver or regenerates synthetic events

#### Scenario: SCN-ML-03.4 — Local training uses the local lake
- **WHEN** training runs without object storage configured
- **THEN** it reads Gold features and labels from the local lake, the same way the
  cluster job reads them from object storage

### Requirement: REQ-ML-04 — Shared model artifact store
Trained models SHALL be persisted to a store shared by the separate train, score
and serving processes — the object store (`s3://<prefix>-ml`, i.e. MinIO/S3) when
S3 is enabled, else a local dir. `train` writes the models; the Score stage and
the scoring API load them from the same store.

#### Scenario: SCN-ML-04.1 — Score reads models from the store
- **WHEN** `train` has written the credit/fraud models to the store and the Score
  stage runs without in-memory models
- **THEN** it loads them from the store and writes `credit_scores`/`fraud_scores`

### Requirement: REQ-ML-05 — Fee-charge features feed the credit model
The credit feature set SHALL include product fee-charge signals (total,
recurring, overage, count, recent window), attributed to a subscriber via the
product's agreement, so product-fee commitment informs the credit decision
(ADR-0023; reverses the analytics-only stance of ADR-0022 for the credit model).

#### Scenario: SCN-ML-05.1 — Fee features attributed to subscribers
- **WHEN** credit features are built on Silver that includes `fact_fee_charge`
- **THEN** the feature table has `fee_total`/`fee_recurring_total`/`fee_overage_count`/
  `fee_charge_count`/`fee_total_30d`, non-zero for subscribers that have fee charges

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

### Requirement: REQ-ML-08 — Idempotent labels and per-run score snapshots
Re-seeding the labels store SHALL replace the labels of the tenants being seeded;
it SHALL NOT append a second copy. Score tables are append-only history (REQ-ML-07).
Each Score run SHALL contribute exactly one row per `(tenant_id, service_id)`, all
stamped with that run's `scored_at`, so any single run can be read back as a
complete, duplicate-free snapshot.

#### Scenario: SCN-ML-08.1 — Re-seeding labels does not stack
- **WHEN** `idp labels` runs twice with the same parameters
- **THEN** the labels store holds exactly one label per `(tenant_id, service_id)`,
  and it is the label from the latest seeding

#### Scenario: SCN-ML-08.2 — One row per subscriber per score run
- **WHEN** the Score stage runs twice
- **THEN** `credit_scores` holds two runs, and filtering on either `scored_at`
  yields exactly one row per `(tenant_id, service_id)`

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
