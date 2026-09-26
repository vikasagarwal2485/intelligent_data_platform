# Spec Delta

## MODIFIED Requirements

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
