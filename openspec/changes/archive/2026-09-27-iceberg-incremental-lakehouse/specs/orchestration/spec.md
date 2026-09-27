# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-ORC-01 — On-demand stage jobs
The system SHALL provide independent, runnable jobs for each stage. Each reads its
input and writes its output, and each is exposed as an `idp` CLI command and as a
Kubernetes Job in `idp-app`:

- `idp produce`: synthetic data → Kafka
- `idp consume`: Kafka → Bronze
- `idp silver`: incremental; `--full-refresh` rebuilds from all of Bronze
- `idp gold`: features only
- `idp score`: Gold features + models → score tables
- `idp publish`: Gold → TimescaleDB serving
- `idp labels`: seed the labels store
- `idp train`
- `idp maintain`: lake maintenance (REQ-ORC-06)

The Silver and Gold stages SHALL run on the single transformation engine
(REQ-ORC-05), whether started from the CLI, a Kubernetes Job or Airflow.

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
The system SHALL provide **four** Airflow DAGs, decoupled by cadence and wired via
Airflow **Datasets**:

- **Features DAG** (`silver >> gold`, hourly): produces the gold-features dataset.
- **Training DAG** (`labels >> train`, daily): produces the model dataset.
- **Inference DAG** (`score >> publish`): dataset-driven, scheduled on
  `(gold-features | model)`, so it re-runs on a feature refresh OR a new model
  without a manual re-trigger.
- **Maintenance DAG** (`maintain`, daily): compacts and expires the lake's tables
  (REQ-ORC-06).

Each stage runs as a pod the DAG launches; Airflow itself needs neither the `idp`
package nor Spark. The Silver and Gold tasks SHALL run the single Spark
implementation of their stage (REQ-ORC-05). There SHALL be no per-stage engine
selection. The features DAG SHALL run at most one DAG run at a time, so a Silver
run never overlaps another run's Silver or Gold. That also keeps the fact
checkpoints single-writer.

#### Scenario: SCN-ORC-02.1 — Inference re-runs on a new model
- **WHEN** the training DAG completes and updates the model dataset
- **THEN** the inference DAG (`score >> publish`) is triggered by that dataset
  update and scores the latest Gold features — no manual re-run

#### Scenario: SCN-ORC-02.2 — Spark Gold parity
- **WHEN** the features DAG's `gold` task runs against Silver
- **THEN** it writes `credit_features`/`fraud_features` to the Gold bucket,
  partitioned by `tenant_id`, with the `(tenant_id, service_id)` keys followed by
  the full credit and fraud model feature sets (the Gold feature contract the
  models score against)

#### Scenario: SCN-ORC-02.3 — Engine switch keeps DAG shape
- **WHEN** the features DAG is parsed with the former `idp_silver_engine` /
  `idp_gold_engine` Variables or `IDP_*_ENGINE` env vars set to any value
- **THEN** the setting has no effect: the tasks are still `silver >> gold`, and
  both run the same `idp` stage commands as the CLI and the Kubernetes Jobs

#### Scenario: SCN-ORC-02.4 — Feature refreshes never overlap
- **WHEN** several features DAG runs are due at once (e.g. a backlog released by
  unpausing the DAG, or a manual trigger during a scheduled run)
- **THEN** they execute one after another, and no Silver task runs while another
  run's Silver or Gold task is running

#### Scenario: SCN-ORC-02.5 — Maintenance runs daily
- **WHEN** the maintenance DAG's daily schedule fires
- **THEN** it runs `idp maintain` as a pod, and does not trigger the features or
  inference DAGs

## ADDED Requirements

### Requirement: REQ-ORC-06 — Lake maintenance
The system SHALL provide a maintenance stage (`idp maintain`) that keeps the lake's
Iceberg tables efficient and bounded:
- it compacts small data files, which the frequent Bronze appends and Silver merges
  produce
- it expires snapshots older than a configurable retention (default 7 days), always
  keeping the current snapshot and any snapshot a checkpoint still needs
- it removes orphan files older than the retention

Maintenance SHALL NOT change any table's current contents. It SHALL NOT delete
Bronze records: Bronze history is kept, and its cost is controlled by an archive
policy that moves Bronze objects older than a configurable age to a colder storage
class, where the object store supports it.

#### Scenario: SCN-ORC-06.1 — Compaction preserves contents
- **WHEN** `idp maintain` runs on a table with many small files
- **THEN** afterwards the table has fewer data files and exactly the same rows

#### Scenario: SCN-ORC-06.2 — Old snapshots expire, current data stays
- **WHEN** `idp maintain` runs on tables with snapshots older than the retention
- **THEN** those snapshots are removed, every table still reads its current
  contents, and the next incremental Silver run still processes only new Bronze

#### Scenario: SCN-ORC-06.3 — Bronze history is never deleted
- **WHEN** `idp maintain` runs
- **THEN** every Bronze record ever landed is still readable
