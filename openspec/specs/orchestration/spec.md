# Orchestration Specification

## Purpose

Run the medallion stages on demand and on a schedule, keeping each stage
independent (ADR-0009).

## Requirements

### Requirement: REQ-ORC-01 — On-demand stage jobs
The system SHALL provide independent, runnable jobs for each stage. Each reads its
input and writes its output, and each is exposed as an `idp` CLI command and as a
Kubernetes Job in `idp-app`:

- `idp produce`: synthetic data → Kafka
- `idp consume`: Kafka → Bronze
- `idp silver`
- `idp gold`: features only
- `idp score`: Gold features + models → score tables
- `idp publish`: Gold → TimescaleDB serving
- `idp labels`: seed the labels store
- `idp train`

The Silver and Gold stages SHALL also be runnable as Spark Jobs.

#### Scenario: SCN-ORC-01.1 — Independent Silver run
- **WHEN** `idp silver` runs against a populated Bronze
- **THEN** it produces Silver on `s3://idp-silver` without touching Gold

#### Scenario: SCN-ORC-01.2 — Independent Gold run
- **WHEN** `idp gold` runs against a populated Silver
- **THEN** it produces Gold feature tables on `s3://idp-gold`

#### Scenario: SCN-ORC-01.3 — Score without a model is not an error
- **WHEN** `idp score` runs before any model has been trained
- **THEN** it writes no score tables, tells the operator to run `idp train`, and
  exits successfully

### Requirement: REQ-ORC-02 — Scheduled orchestration (Airflow) and production Gold (Spark)
The system SHALL provide **three** Airflow DAGs, decoupled by cadence and wired via
Airflow **Datasets**:

- **Features DAG** (`silver >> gold`, hourly): produces the gold-features dataset.
- **Training DAG** (`labels >> train`, daily): produces the model dataset.
- **Inference DAG** (`score >> publish`): dataset-driven, scheduled on
  `(gold-features | model)`, so it re-runs on a feature refresh OR a new model
  without a manual re-trigger.

Each stage runs as a pod the DAG launches; Airflow itself needs neither the `idp`
package nor Spark. The Silver and Gold tasks SHALL each have an independent
execution-engine switch (`pandas` or `spark`), set by an Airflow Variable or an env
var and defaulting to `pandas`. Switching engines SHALL leave the DAG shape and task
ids unchanged. PySpark jobs SHALL implement the Silver and Gold stages for
production scale.

#### Scenario: SCN-ORC-02.1 — Inference re-runs on a new model
- **WHEN** the training DAG completes and updates the model dataset
- **THEN** the inference DAG (`score >> publish`) is triggered by that dataset
  update and scores the latest Gold features — no manual re-run

#### Scenario: SCN-ORC-02.2 — Spark Gold parity
- **WHEN** the Spark Gold job runs against Silver
- **THEN** it writes `credit_features`/`fraud_features` to the Gold bucket,
  partitioned by `tenant_id`

#### Scenario: SCN-ORC-02.3 — Engine switch keeps DAG shape
- **WHEN** `idp_gold_engine` is set to `spark`
- **THEN** the features DAG still has tasks `silver >> gold`, and the `gold` task
  runs the Spark Gold job instead of `idp gold`

### Requirement: REQ-ORC-03 — Local composition remains available
The system SHALL keep a local `run` orchestrator that composes the same stage
functions (generate → bronze → silver → labels → gold → train → score) for
development and testing and for offline model training.

#### Scenario: SCN-ORC-03.1 — Local end-to-end
- **WHEN** `idp run` executes locally
- **THEN** all layers and trained models are produced under `data/lake` and
  `ml/artifacts` without a cluster

### Requirement: REQ-ORC-04 — Engine output parity (pandas ↔ Spark)
For the same input layer, the Spark implementation of a stage SHALL produce the
same tables, with the same column names, order and logical types, as the pandas
implementation. Specifically:

- **Spark Silver** SHALL produce `dim_subscriber`, `fact_usage`, `fact_recharge` and
  `fact_fee_charge` (REQ-SLV-01/05), with identical tokenized identifiers.
- **Spark Gold** SHALL produce `credit_features` and `fraud_features` with the
  `(tenant_id, service_id)` keys followed by the full credit and fraud model feature
  sets, using the configured lookback windows.

Integer-valued columns (counts, days) SHALL match exactly. Real-valued columns SHALL
match within an absolute tolerance of 1e-6 (rounded columns: within their rounding
unit). Spark writes SHALL replace only the partitions of the tenants being written.

#### Scenario: SCN-ORC-04.1 — Silver parity
- **WHEN** the pandas and Spark Silver stages run on the same Bronze with the same
  tenant salts
- **THEN** both produce the four Silver tables with identical schemas, identical
  `service_id` tokens, and equal row counts per tenant

#### Scenario: SCN-ORC-04.2 — Gold parity
- **WHEN** the pandas and Spark Gold stages run on the same Silver
- **THEN** both `credit_features` tables have the same columns in the same order,
  one row per `(tenant_id, service_id)`, and matching values within tolerance, and
  likewise for `fraud_features`

#### Scenario: SCN-ORC-04.3 — Spark-built Gold is scoreable
- **WHEN** Gold was produced by the Spark engine and `idp score` then runs with
  trained models
- **THEN** scoring succeeds and writes `credit_scores` and `fraud_scores`

#### Scenario: SCN-ORC-04.4 — Spark replaces per tenant
- **WHEN** a Spark stage writes output containing only `tenant_a`
- **THEN** the `tenant_b` partitions of that table are unchanged
