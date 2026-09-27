# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-ORC-01 — On-demand stage jobs
The system SHALL provide independent, runnable jobs for each stage. Each reads its
input and writes its output, and each is exposed as an `idp` CLI command and as a
cluster job in `idp-app`:

- `idp produce`: synthetic data → Kafka
- `idp consume`: Kafka → Bronze
- `idp silver`: incremental; `--full-refresh` rebuilds from all of Bronze
- `idp gold`: features only
- `idp score`: Gold features + models → score tables
- `idp publish`: Gold → TimescaleDB serving
- `idp labels`: seed the labels store
- `idp train`
- `idp maintain`: lake maintenance (REQ-ORC-06)

The Silver, Gold, Score, publish and maintenance stages SHALL run on the single
transformation engine (REQ-ORC-05). In the cluster they SHALL run as Spark
applications, with one driver and executors, submitted through the Spark Operator.
The other stages run as Kubernetes Jobs.

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

Airflow itself needs neither the `idp` package nor Spark. The Spark stages'
tasks (silver, gold, score, publish, maintain) SHALL submit a Spark application
through the Spark Operator and succeed only when that application completes. The
other tasks run as pods. The Silver and Gold tasks SHALL run the single Spark
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

### Requirement: REQ-ORC-05 — Single transformation engine; pandas confined to ML
The Silver and Gold transforms SHALL have exactly one implementation, on Apache
Spark, shared by the CLI stages, the cluster jobs, the Airflow tasks and the tests.
The execution environment SHALL only select the Spark master and its resources:
- `local[*]` by default (local runs and CI)
- Kubernetes through the Spark Operator in the cluster
- overridable by configuration

No stage SHALL materialize a whole lake table into single-process (pandas) memory.
pandas SHALL be used only:
- per partition, inside distributed scoring
- for model training, on a bounded sample (REQ-ML-09)
- by the synthetic data generator
- by the Python Bronze landing, per consumed batch

#### Scenario: SCN-ORC-05.1 — Same code in every entrypoint
- **WHEN** Silver is built once with `idp silver` locally and once by the Silver
  Spark application in the cluster, from equal Bronze
- **THEN** both run the same transform code and produce equal Silver tables; only
  the Spark master and resources differ

#### Scenario: SCN-ORC-05.2 — Master from configuration
- **WHEN** no Spark master is configured
- **THEN** the stages run on `local[*]`; **AND WHEN** a master is configured
  (e.g. via `IDP_SPARK_MASTER`) **THEN** the stages use it

#### Scenario: SCN-ORC-05.3 — No whole-table pandas in Silver/Gold
- **WHEN** the Silver, Gold, Score or publish stage runs
- **THEN** it completes without converting a whole input or output table to a
  pandas DataFrame

## ADDED Requirements

### Requirement: REQ-ORC-07 — Resource profiles and scale-out
Each Spark stage SHALL run with an explicit resource profile from configuration:
- driver cores, memory and memory overhead
- executor cores, memory and memory overhead
- either a fixed executor count or dynamic-allocation bounds

Every driver and executor container's memory limit SHALL cover its JVM heap plus
the declared overhead. A stage's capacity SHALL grow by adding executors, without
changing code. Overlapping runs of different stages SHALL NOT exceed their
containers' limits.

#### Scenario: SCN-ORC-07.1 — Profile applied
- **WHEN** the Gold profile sets 3 executors with 2 GiB memory and 512 MiB overhead
  each
- **THEN** the Gold application runs 3 executor pods whose memory limits are at
  least 2.5 GiB, and the driver uses its own profile

#### Scenario: SCN-ORC-07.2 — More data, more executors
- **WHEN** the lake holds 10× the subscribers and the Silver profile's executor
  count is raised
- **THEN** Silver completes without any container being OOMKilled, with no code
  change

#### Scenario: SCN-ORC-07.3 — Overlapping stages stay within limits
- **WHEN** a Silver application and a maintenance application run at the same time
- **THEN** both complete and no container is OOMKilled
