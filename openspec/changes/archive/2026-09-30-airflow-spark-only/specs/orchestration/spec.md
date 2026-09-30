# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-ORC-01 — On-demand stage jobs
The system SHALL provide independent, runnable jobs for each stage. Each reads its
input and writes its output, and each is exposed as an `idp` CLI command and as a
cluster job in `idp-app`:

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

The Silver, Gold, Score, publish and maintenance stages SHALL run on the single
transformation engine (REQ-ORC-05). In the cluster they, and the `train` stage,
SHALL run as Spark applications, with one driver and executors, submitted through
the Spark Operator: `train` reads, joins and samples its training set on the
executors and fits the models on the driver, from the bounded sample (REQ-ML-09).
The other stages (generate, produce, consume, labels) run as Kubernetes Jobs.

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

#### Scenario: SCN-ORC-01.4 — Spark stages run with executors in the cluster
- **WHEN** `make silver` (or the Airflow `silver` task) runs in the cluster
- **THEN** a Spark application with one driver pod and at least one executor pod
  runs the Silver stage, and its result equals a `local[*]` run on the same input

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

#### Scenario: SCN-ORC-01.8 — Train runs as a Spark application
- **WHEN** `make train` (or the Airflow `train` task) runs in the cluster
- **THEN** a Spark application with one driver pod and at least one executor pod
  trains both models on the bounded sample and persists them to the model store
  (REQ-ML-04), with the same metrics a `local[*]` run reports on the same input

### Requirement: REQ-ORC-02 — Scheduled orchestration (Airflow) and production Gold (Spark)
The system SHALL provide **four** Airflow DAGs, decoupled by cadence and wired via
Airflow **Datasets**:

- **Features DAG** (`silver >> gold`, hourly): produces the gold-features dataset.
- **Training DAG** (`train`, daily): produces the model dataset. It SHALL NOT seed
  the labels store: labels are seeded outside Airflow (`make labels`), or supplied
  by an outcomes feed.
- **Inference DAG** (`score >> publish`): dataset-driven, scheduled on
  `(gold-features | model)`, so it re-runs on a feature refresh OR a new model
  without a manual re-trigger.
- **Maintenance DAG** (`maintain`, daily): compacts and expires the lake's tables
  (REQ-ORC-06).

Airflow itself needs neither the `idp` package nor Spark. Every Airflow task
(silver, gold, score, publish, maintain, train) SHALL submit a Spark application
through the Spark Operator and succeed only when that application completes; no
task SHALL start a pod directly. The Silver and Gold tasks SHALL run the single Spark
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
  both run the same `idp` stage code as the CLI and the cluster jobs

#### Scenario: SCN-ORC-02.4 — Feature refreshes never overlap
- **WHEN** several features DAG runs are due at once (e.g. a backlog released by
  unpausing the DAG, or a manual trigger during a scheduled run)
- **THEN** they execute one after another, and no Silver task runs while another
  run's Silver or Gold task is running

#### Scenario: SCN-ORC-02.5 — Maintenance runs daily
- **WHEN** the maintenance DAG's daily schedule fires
- **THEN** it runs `idp maintain` as a Spark application, and does not trigger the
  features or inference DAGs

#### Scenario: SCN-ORC-02.6 — A failed Spark application fails the task
- **WHEN** a Spark stage's application ends in a failed state (e.g. a driver error)
- **THEN** the Airflow task fails, retries per its policy, and downstream tasks do
  not run

#### Scenario: SCN-ORC-02.7 — Training needs seeded labels
- **WHEN** the training DAG runs and the labels store is empty
- **THEN** the `train` task fails with a message to seed the labels (`make labels`),
  the model dataset is not updated, and the inference DAG is not triggered

#### Scenario: SCN-ORC-02.8 — Airflow only submits Spark applications
- **WHEN** the four DAGs are parsed
- **THEN** every task submits a Spark application from its rendered manifest, and
  no task launches a pod of its own
