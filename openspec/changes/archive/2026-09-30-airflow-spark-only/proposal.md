# Proposal

## Why

Four of the five Airflow task types already submit Spark Operator applications
(silver, gold, score, publish, maintain). Only the training DAG still launches plain
pods through `KubernetesPodOperator`, for two tasks:

- **`train`.** It already runs Spark inside its pod: a local session reads the Gold
  features, joins the labels and takes the bounded, stratified sample (REQ-ML-09).
  Then it fits scikit-learn on that sample. A local session limits the join and
  sampling to one container, while every other Spark stage gets executors.
- **`labels`.** It is not Spark work. It regenerates the synthetic outcomes with
  pandas and appends them to the labels store. The generation is deterministic, so
  the daily run rewrites the same labels (and stacks duplicate copies that the reader
  drops). It also depends on data-volume settings hard-coded in the DAG
  (`IDP_SUBSCRIBERS`/`IDP_DAYS`), which must stay in step with the producer. It is a
  stand-in for a real outcomes feed, which Airflow wouldn't generate either.

So Airflow carries a second operator, its own pod env and resource settings, and
permission to create pods in `idp-app`, only for these two tasks. Moving `train` to a
Spark application and taking `labels` out of Airflow leaves one way to run a task:
submit a Spark application.

## What Changes

- **`train` becomes a Spark stage.** It runs as a Spark Operator application (driver
  plus executors) from its `spark.profiles.train` profile, like the other Spark
  stages. The join and the stratified sample run on the executors; fitting stays on
  the driver, on the bounded sample. A new entrypoint `pipelines/spark/train_job.py`
  wraps `idp train`, and `deploy/app/spark/train.yaml` is rendered by
  `make spark-manifests`. `make train` submits the application and waits for it.
- **The training DAG runs `train` only.** It still produces the model dataset that
  triggers inference.
- **BREAKING (operations):** the training DAG no longer seeds the labels store.
  Labels are seeded on demand with `make labels` (the existing Kubernetes Job), next
  to data generation. If they were never seeded, the `train` task fails with the
  existing message telling the operator to seed them.
- **Airflow submits only Spark applications.** The DAG drops `KubernetesPodOperator`
  and its pod helper, env lists and train resources. Airflow's RBAC in `idp-app`
  keeps only the `SparkApplication` permissions; the pod permissions are removed.
- The non-Spark stages (generate, produce, consume, labels) stay CLI stages run as
  Kubernetes Jobs by `make`. The old `train-job.yaml` Job stays for rollback, as the
  other pre-Operator Jobs did.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `orchestration`: REQ-ORC-01 adds `train` to the stages that run as Spark
  applications in the cluster (labels stays a Job). REQ-ORC-02 makes the training DAG
  `train` only, and requires every Airflow task to submit a Spark application.
- `platform-deployment`: REQ-PLT-10 limits the Airflow ServiceAccount in `idp-app` to
  `SparkApplication`s, with no permission to create pods directly.

## Impact

- Code: `src/idp/transforms/sparkapp.py` (`train` in `SPARK_STAGES`),
  `pipelines/spark/train_job.py` (new), `config/platform.yaml`
  (`spark.profiles.train` gains an executor).
- Orchestration: `pipelines/airflow/dags/idp_medallion_dag.py` (no pod operator; the
  training DAG is `train` only); `deploy/app/spark/train.yaml` (new, rendered, and
  baked into the DAG image like the others).
- Deploy: `deploy/platform/airflow/rbac.yaml` (the pod Role and RoleBinding are
  removed); `Makefile` (`make train` via the Spark Operator; `spark-manifests`
  renders `train`).
- Tests: `tests/test_dags.py`, `tests/test_sparkapp.py`.
- Docs: `README.md`, `deploy/README.md`.
- Node memory: the train application (driver 2 GiB + 1 GiB overhead, one 1 GiB
  executor) must fit next to the platform; `make mk-check` covers it.
