# Orchestration Specification

## Purpose

Run the medallion stages on demand and on a schedule, keeping each stage
independent (ADR-0009).

## Requirements

### Requirement: REQ-ORC-01 — On-demand stage jobs
The system SHALL provide independent, runnable jobs for each stage. Each reads its
input and writes its output, and each is exposed as an `idp` CLI command and as a
Kubernetes Job in `idp-app`:

- `idp generate`: synthetic source files → landing zone (REQ-BRZ-03)
- `idp produce`: synthetic data → Kafka; with `--from-landing`, the landing files →
  Kafka (REQ-STR-01)
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

Streaming ingestion SHALL be runnable end to end with one command, in two variants:
- **`make stream`:** in-memory `idp produce`, then `idp consume`
- **`make stream-from-landing`:** `idp generate` (landing zone), then
  `idp produce --from-landing`, then `idp consume`

Each variant SHALL run its steps as Jobs in `idp-app`, in order, starting a step
only after the previous one has completed, and SHALL stop with a failure at the
first failed step. `make stream-replay` SHALL re-publish the existing landing files
and consume them, without regenerating.

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

#### Scenario: SCN-ORC-01.5 — In-memory streaming unchanged
- **WHEN** `make stream` runs
- **THEN** it produces the generated data to Kafka in memory and consumes it into
  Bronze, without reading or writing the landing zone

#### Scenario: SCN-ORC-01.6 — Streaming from the landing zone
- **WHEN** `make stream-from-landing` runs
- **THEN** it writes the landing files to `s3://idp-landing`, publishes them to
  Kafka, then consumes them into Bronze, in that order; Bronze holds exactly the
  landing records, per tenant and entity

#### Scenario: SCN-ORC-01.7 — A rejected landing file stops the chain
- **WHEN** the landing zone holds a file whose records don't match its tenant, and
  `make stream-from-landing` or `make stream-replay` runs
- **THEN** the produce step fails and reports the file, consume doesn't run, and
  the command exits with a failure status

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

### Requirement: REQ-ORC-03 — Local composition remains available
The system SHALL keep a local `run` orchestrator that composes the same stage
functions (generate → bronze → silver → labels → gold → train → score) for
development, testing and offline model training. Each stage SHALL read its input
from the lake written by the previous stage (a local lake directory when object
storage is not configured), exactly as in the cluster, and the Silver and Gold
stages SHALL run on a local Spark session. Stages SHALL NOT hand data to each other
in memory.

#### Scenario: SCN-ORC-03.1 — Local end-to-end
- **WHEN** `idp run` executes locally with a JDK available and no object storage
  configured
- **THEN** all layers and trained models are produced under `data/lake` and
  `ml/artifacts` without a cluster

#### Scenario: SCN-ORC-03.2 — Stages compose through the lake
- **WHEN** `idp run` completes
- **THEN** every layer it produced can be read back from the local lake, and the
  Gold feature tables equal those produced by running `idp silver` then `idp gold`
  separately on the same Bronze

### Requirement: REQ-ORC-05 — Single transformation engine; pandas confined to ML
The Silver and Gold transforms SHALL have exactly one implementation, on Apache
Spark, shared by the CLI stages, the Kubernetes Jobs, the Airflow tasks and the
tests. The execution environment SHALL only select the Spark master and its
resources. It SHALL default to a local master (`local[*]`) and be overridable by
configuration. The Silver and Gold stages SHALL NOT materialize a whole table into
single-process (pandas) memory. pandas SHALL be used only for ML: training on the
labelled Gold feature set, scoring, and the synthetic data generator. The Bronze
landing, publish and MCP reads are unchanged for now.

#### Scenario: SCN-ORC-05.1 — Same code in every entrypoint
- **WHEN** Silver is built once with `idp silver` locally and once by the Silver
  Kubernetes Job in the cluster, from equal Bronze
- **THEN** both run the same transform code and produce equal Silver tables; only
  the Spark master and resources differ

#### Scenario: SCN-ORC-05.2 — Master from configuration
- **WHEN** no Spark master is configured
- **THEN** the stages run on `local[*]`; **AND WHEN** a master is configured
  (e.g. via `IDP_SPARK_MASTER`) **THEN** the stages use it

#### Scenario: SCN-ORC-05.3 — No whole-table pandas in Silver/Gold
- **WHEN** the Silver or Gold stage runs
- **THEN** it completes without converting any input or output table to a pandas
  DataFrame

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
