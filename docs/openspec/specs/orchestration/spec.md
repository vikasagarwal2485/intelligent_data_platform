# Orchestration Specification

## Purpose

Run the medallion stages on demand and on a schedule, keeping each stage
independent (ADR-0009).

## Requirements

### Requirement: REQ-ORC-01 — On-demand stage jobs
The system SHALL provide independent, runnable jobs for each stage —
`idp consume` (Bronze), `idp silver`, `idp gold`, `idp publish` (Gold →
TimescaleDB serving), `idp train` — each reading its input layer and writing its
output, exposed as CLI commands and as Kubernetes Jobs in `idp-app`.

#### Scenario: SCN-ORC-01.1 — Independent Silver run
- **WHEN** `idp silver` runs against a populated Bronze
- **THEN** it produces Silver on `s3://idp-silver` without touching Gold

#### Scenario: SCN-ORC-01.2 — Independent Gold run
- **WHEN** `idp gold` runs against a populated Silver
- **THEN** it produces Gold on `s3://idp-gold`

### Requirement: REQ-ORC-02 — Scheduled orchestration (Airflow) and production Gold (Spark)
The system SHALL provide **three** Airflow DAGs, decoupled by cadence and wired via
Airflow **Datasets**: a **features** DAG (`silver >> gold`) that produces the
gold-features dataset; a **training** DAG (`labels >> train`) that produces the
model dataset; and a **dataset-driven inference** DAG (`score >> publish`) scheduled
on `(gold-features | model)` so it re-runs on a feature refresh OR a new model
without a manual re-trigger. Plus a PySpark job implementing the Gold stage for
production scale.

#### Scenario: SCN-ORC-02.1 — Inference re-runs on a new model
- **WHEN** the training DAG completes and updates the model dataset
- **THEN** the inference DAG (`score >> publish`) is triggered by that dataset
  update and scores the latest Gold features — no manual re-run

#### Scenario: SCN-ORC-02.2 — Spark Gold parity
- **WHEN** the Spark Gold job runs against Silver
- **THEN** it writes `credit_features`/`fraud_features` to the Gold bucket,
  partitioned by `tenant_id`

### Requirement: REQ-ORC-03 — Local composition remains available
The system SHALL keep a local `run` orchestrator that composes the same stage
functions (generate → bronze → silver → labels → gold → train → score) for
development and testing and for offline model training.

#### Scenario: SCN-ORC-03.1 — Local end-to-end
- **WHEN** `idp run` executes locally
- **THEN** all layers and trained models are produced under `data/lake` and
  `ml/artifacts` without a cluster
