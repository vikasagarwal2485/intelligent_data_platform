# Orchestration Specification

## Purpose

Run the medallion stages on demand and on a schedule, keeping each stage
independent (ADR-0009).

## Requirements

### Requirement: On-demand stage jobs
The system SHALL provide independent, runnable jobs for each stage —
`idp consume` (Bronze), `idp silver`, `idp gold`, `idp publish` (Gold →
TimescaleDB serving), `idp train` — each reading its input layer and writing its
output, exposed as CLI commands and as Kubernetes Jobs in `idp-app`.

#### Scenario: Independent Silver run
- **WHEN** `idp silver` runs against a populated Bronze
- **THEN** it produces Silver on `s3://idp-silver` without touching Gold

#### Scenario: Independent Gold run
- **WHEN** `idp gold` runs against a populated Silver
- **THEN** it produces Gold on `s3://idp-gold`

### Requirement: Scheduled orchestration (Airflow) and production Gold (Spark)
The system SHALL provide an Airflow DAG that orchestrates `silver >> gold` on a
schedule plus a separate daily training DAG, and a PySpark job implementing the
Gold stage for production scale.

#### Scenario: DAG chains Silver then Gold
- **WHEN** the medallion DAG runs
- **THEN** the `silver` task completes before `gold`, each reading/writing S3

#### Scenario: Spark Gold parity
- **WHEN** the Spark Gold job runs against Silver
- **THEN** it writes `credit_features`/`fraud_features` to the Gold bucket,
  partitioned by `tenant_id`

### Requirement: Local composition remains available
The system SHALL keep a local `run` orchestrator that composes the same stage
functions (generate → bronze → silver → train → gold) for development and testing
and for offline model training.

#### Scenario: Local end-to-end
- **WHEN** `idp run` executes locally
- **THEN** all layers and trained models are produced under `data/lake` and
  `ml/artifacts` without a cluster
