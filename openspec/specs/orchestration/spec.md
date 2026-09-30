# Orchestration Specification

## Purpose

Run the medallion stages on demand and on a schedule, keeping each stage
independent (ADR-0009).

## Requirements

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
This SHALL be their only cluster execution path, both for manual runs (`make
<stage>`) and for Airflow: each is submitted from its manifest rendered from
`spark.profiles.<stage>`, and there SHALL be no alternate Kubernetes Job for a Spark
stage. The other stages (generate, produce, consume, labels, and the one-off lake
migration) run as Kubernetes Jobs.

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

#### Scenario: SCN-ORC-01.9 — One cluster path per Spark stage
- **WHEN** the repository's cluster manifests are inspected
- **THEN** each Spark stage (silver, gold, score, publish, maintain, train) has
  exactly one: its rendered SparkApplication, which both `make <stage>` and the
  Airflow task submit. The only Kubernetes Jobs are the non-Spark stages the `make`
  targets apply (generate, produce, produce from landing, consume, labels, lake
  migration)

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
- to shape the bounded results of analytics queries (REQ-MCP-09)

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

### Requirement: REQ-ORC-07 — Resource profiles and scale-out
Each Spark stage SHALL run with an explicit resource profile from configuration:
- driver cores, memory and memory overhead
- executor cores, memory and memory overhead
- either a fixed executor count or dynamic-allocation bounds

Every driver and executor container's memory limit SHALL cover its JVM heap plus
the declared overhead. A stage's capacity SHALL grow by adding executors, without
changing code. Overlapping runs of different stages SHALL NOT exceed their
containers' limits.

Each stage's application manifest SHALL be rendered from its profile into a single
committed copy (`deploy/app/spark/<stage>.yaml`). Both the manual `make` runs and
the Airflow tasks SHALL submit that copy; the Airflow DAG image SHALL take it from
there at build time rather than keep its own.

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

#### Scenario: SCN-ORC-07.4 — One manifest for make and Airflow
- **WHEN** a stage's profile changes and the manifests are re-rendered
  (`make spark-manifests`)
- **THEN** only `deploy/app/spark/<stage>.yaml` changes, it matches the profile, and
  the next DAG image build (`make airflow-reload`) submits that same manifest
