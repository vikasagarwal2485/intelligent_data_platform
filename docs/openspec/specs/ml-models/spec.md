# ML Models Specification

## Purpose

Alternative credit-limit scoring and fraud detection, plus offline training that
is separate from the ELT (labels never enter the lake — ADR-0014).

## Requirements

### Requirement: Credit-limit model (PD + affordability sizing)
The system SHALL train a probability-of-default classifier (gradient-boosted;
`HistGradientBoostingClassifier` standing in for LightGBM/XGBoost) with an
interpretable logistic-scorecard baseline, and SHALL size a recommended limit as
`affordability(recharge, tenure) × (1 − PD)` clipped to policy bands.

#### Scenario: PD is a probability
- **WHEN** the credit model scores a feature frame
- **THEN** every predicted PD is within [0, 1]

#### Scenario: Higher risk, not-higher limit
- **WHEN** `recommend_limit` is applied
- **THEN** the mean recommended limit for high-PD subscribers is ≤ that of low-PD
  subscribers, and every limit is within the configured [min, max] band

#### Scenario: Out-of-sample evaluation
- **WHEN** the credit model is evaluated on a holdout split
- **THEN** it reports AUC, KS, and Gini, with AUC clearly above 0.5

### Requirement: Fraud model (layered, fused score)
The system SHALL combine deterministic rules, a gradient-boosted classifier
(imbalance-weighted, optimized for PR-AUC), and an IsolationForest anomaly score
into a single fused fraud score in [0, 1].

#### Scenario: Rules flag SIM-box
- **WHEN** a subscriber exceeds the originating-ratio and call-velocity thresholds
- **THEN** the SIM-box rule flag is set and contributes to the fused score

#### Scenario: Fraud ranked above normal
- **WHEN** the fraud model scores a labelled frame
- **THEN** the mean fraud score for fraud subscribers exceeds that for normal ones,
  and out-of-sample PR-AUC exceeds the base rate

### Requirement: Training is a separate, offline stage
The `train` stage SHALL build features from Silver, join ground-truth labels
(available only offline), fit both models with an out-of-sample split, and
persist the models to the artifact store (MLflow in production).

#### Scenario: Models persisted
- **WHEN** `train` runs with `persist=True`
- **THEN** `credit_model` and `fraud_model` artifacts are written and reload to
  produce identical predictions

#### Scenario: Explainability available
- **WHEN** a credit decision is produced
- **THEN** per-decision reason codes (e.g. SHAP) can be generated for adverse action
