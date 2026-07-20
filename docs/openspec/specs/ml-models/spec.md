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
The `train` stage SHALL build features from Silver, join ground-truth labels
(available only offline), fit both models with an out-of-sample split, and
persist the models to the artifact store (MLflow in production).

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

### Requirement: REQ-ML-04 — Shared model artifact store
Trained models SHALL be persisted to a store shared by the separate train, Gold
and serving processes — the object store (`s3://<prefix>-ml`, i.e. MinIO/S3) when
S3 is enabled, else a local dir. `train` writes the models; `to_gold` and the
scoring API load them from the same store; when a model is absent Gold SHALL still
produce the feature tables (graceful degrade).

#### Scenario: SCN-ML-04.1 — Gold scores from the store
- **WHEN** `train` has written the credit/fraud models to the store and `to_gold`
  runs without in-memory models
- **THEN** it loads them from the store and Gold contains `credit_scores` and
  `fraud_scores`

#### Scenario: SCN-ML-04.2 — Graceful degrade without models
- **WHEN** `to_gold` runs and no model exists in the store
- **THEN** Gold contains the feature tables only, without raising

### Requirement: REQ-ML-05 — Fee-charge features feed the credit model
The credit feature set SHALL include product fee-charge signals (total,
recurring, overage, count, recent window), attributed to a subscriber via the
product's agreement, so product-fee commitment informs the credit decision
(ADR-0023; reverses the analytics-only stance of ADR-0022 for the credit model).

#### Scenario: SCN-ML-05.1 — Fee features attributed to subscribers
- **WHEN** credit features are built on Silver that includes `fact_fee_charge`
- **THEN** the feature table has `fee_total`/`fee_recurring_total`/`fee_overage_count`/
  `fee_charge_count`/`fee_total_30d`, non-zero for subscribers that have fee charges
