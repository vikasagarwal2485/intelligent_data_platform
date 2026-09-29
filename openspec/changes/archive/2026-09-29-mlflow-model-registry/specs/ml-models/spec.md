# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-ML-04 — Shared model artifact store
Trained models SHALL be managed in a model registry (MLflow) shared by the separate
train, score and serving processes.

- **Train:** each `train` run SHALL log its parameters, hold-out metrics and model
  artifacts, and register a new version of each model (`idp-credit`, `idp-fraud`).
  A new version SHALL be promoted to the `champion` alias only when it passes the
  configured quality gate: credit AUC ≥ `credit.min_auc`, fraud PR-AUC ≥
  `fraud.min_pr_auc_lift` × base fraud rate. A version that fails the gate SHALL be
  registered with its metrics but SHALL NOT replace the current champion.
- **Score and API:** the Score stage and the scoring API SHALL load the current
  `champion` version. The scoring API SHALL serve a newly promoted champion without
  a restart, within the configured refresh interval.
- **Fallback:** when no registry is configured, models SHALL be persisted to and
  loaded from the object store (`s3://<prefix>-ml`) when S3 is enabled, else from a
  local directory. In that case the latest trained model is used.

#### Scenario: SCN-ML-04.1 — Score reads models from the store
- **WHEN** `train` has written the credit/fraud models to the store and the Score
  stage runs without in-memory models
- **THEN** it loads them from the store and writes `credit_scores`/`fraud_scores`

#### Scenario: SCN-ML-04.2 — Training run is logged and registered
- **WHEN** `train` runs with a registry configured
- **THEN** a registry run exists with the credit and fraud hold-out metrics, and new
  versions of `idp-credit` and `idp-fraud` are registered from that run

#### Scenario: SCN-ML-04.3 — Gate-passing model becomes champion
- **WHEN** a newly trained credit model's AUC meets `credit.min_auc`
- **THEN** that version holds the `champion` alias, and the next Score run uses it

#### Scenario: SCN-ML-04.4 — Gate-failing model is not promoted
- **WHEN** a newly trained model fails its quality gate
- **THEN** it is registered with its metrics, the previous `champion` is unchanged,
  and Score keeps using the previous champion (or skips that score table if no
  champion exists yet)

#### Scenario: SCN-ML-04.5 — API picks up a new champion
- **WHEN** a new version is promoted to `champion` while the scoring API is running
- **THEN** within the configured refresh interval, API responses are produced by the
  new version without restarting the API

#### Scenario: SCN-ML-04.6 — Fallback without a registry
- **WHEN** no registry is configured
- **THEN** `train` writes the models to the object store/local dir, and Score and
  the API load them from there
