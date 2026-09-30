# Design

## Context

- **Spark stages today** (see proposal.md, Why):
  - silver, gold, score, publish and maintain are listed in
    `sparkapp.SPARK_STAGES`;
  - `idp spark-manifest <stage>` renders each one from `spark.profiles.<stage>`, and
    `make spark-manifests` writes the one committed copy,
    `deploy/app/spark/<stage>.yaml`;
  - `make <stage>` applies it (`run_sparkapp`/`wait_sparkapp`) and prints the driver
    log;
  - the DAG image bakes the folder in as `dags/spark/`, where
    `SparkKubernetesOperator` reads it;
  - `make mk-check` sizes the largest profile against the node.
- **`train` today** is `idp train` in a single pod (`train-job.yaml`, 3 GiB limit), on
  a local Spark session (`spark.profiles.train` has a driver only: 2 GiB heap +
  1 GiB overhead). The session reads Gold and the labels store, joins them and runs
  `stratified_cap`. Then the bounded sample goes to pandas (`toPandas`), and
  scikit-learn fits on the driver. The result is saved through the model store
  (MLflow, REQ-ML-04).
- **The driver side is ready.** `get_spark()` inside a Spark Operator driver already
  keeps the operator's master and resources (`builder_conf` skips them when
  `PYSPARK_GATEWAY_PORT` is set). The Spark apps' env (`sparkapp.LAKE_ENV`) already
  carries S3, the Iceberg catalog, `IDP_SPARK_MASTER` and `IDP_MLFLOW_TRACKING_URI`.
- **Airflow's RBAC** (`deploy/platform/airflow/rbac.yaml`) has two Roles in `idp-app`:
  - `idp-pod-launcher`: pods, `pods/log` and `pods/exec`, with every verb;
  - `idp-spark-submitter`: `sparkapplications`, with every verb.

## Goals / Non-Goals

**Goals:**
- One submission path for Airflow: every task is a `SparkApplication` rendered from a
  profile.
- `train` behaves exactly as before (same sample, metrics, registry and gate); only
  where its Spark work runs changes.

**Non-Goals:**
- Distributing the model fitting (scikit-learn stays on the driver, on the bounded
  sample; Spark ML is out of scope).
- Changing the labels store, how labels are generated, or `make labels`.
- Moving generate, produce or consume onto Spark.

## Decisions

### 1. `train` joins `SPARK_STAGES`, reusing the stage machinery
- `train` is added to `sparkapp.SPARK_STAGES`.
- New entrypoint `pipelines/spark/train_job.py`, the same thin wrapper as
  `score_job.py`: `main(["train", *argv])`.
- `make spark-manifests` renders `deploy/app/spark/train.yaml`, and the DAG image
  picks it up through the existing named build context.
- `make train` becomes `mk-check` + `run_sparkapp,train`, like `make score`.
- `mk-check` and the manifest tests cover it automatically, because both iterate
  `SPARK_STAGES`.
- *Alternative:* a driver-only application (0 executors) to mirror today's local
  session. Rejected: the Spark Operator and REQ-ORC-01.4 expect executors, and the
  join and sample are the part that should scale.

### 2. The `train` profile gets one executor; the driver stays large
`spark.profiles.train` becomes:
- driver: 1 core, `memory: 2g`, `memoryOverhead: 1g`;
- executor: `instances: 1`, 1 core, `memory: 1g`, `memoryOverhead: 512m`.

The driver keeps its size because it holds the collected sample (at most
`ml.train_max_rows` rows, REQ-ML-09) and runs the fit. The total is 4.5 GiB, below
the largest stage (score, 5 GiB), so the node headroom is unchanged. EKS profiles can
add executors for the join; the sample bound keeps the driver fixed.

### 3. Fitting on the driver is unchanged
`pipeline.train` already does its Spark work through the session and only
materializes the bounded sample. As a driver of an operator application it behaves
the same, just with remote executors. No code change is needed in `train()`: the
metrics (`sampled_rows`, `sample_seed`) and the MLflow run are produced exactly as
before.

### 4. The training DAG is `train` only; `labels` leaves Airflow
- `idp_training` = `_spark_stage("train", "train")`, with `outlets = [MODEL]`.
- The DAG drops:
  - the `KubernetesPodOperator` import and the `_stage` helper;
  - `S3_ENV`/`CATALOG_ENV`/`MLFLOW_ENV`/`LAKE_ENV`, `SYN_ENV` and `TRAIN_RESOURCES`.
  That env now lives only in the rendered manifests (`sparkapp.LAKE_ENV`).
- Labels are seeded by `make labels` (the existing Job), with the same synthetic
  params as the producer. A missing labels store makes `train` fail with its existing
  "seed labels (make labels) first" error, so the task fails and `MODEL` isn't
  updated (SCN-ORC-02.7).
- *Alternative:* run `labels` as a `SparkApplication` too. Rejected: its work is
  pandas on one process, so executors would sit idle. And a deterministic daily
  re-seed is exactly the redundancy the change removes.

### 5. Airflow RBAC: submit applications, read pods, create none
- Drop the `idp-pod-launcher` Role and RoleBinding.
- Add read access to `idp-spark-submitter`: pods (get, list, watch) and `pods/log`
  (get). `SparkKubernetesOperator` follows the driver pod and streams its log
  (`get_logs=True`); without read access the task can't report the log.
- Verify with `kubectl auth can-i`, as each Airflow ServiceAccount (scheduler and
  worker):
  - `create pods -n idp-app` → no;
  - `create sparkapplications` and `get pods/log` → yes (SCN-PLT-10.3).
- **Checked against the installed provider** (cncf-kubernetes 8.3.3):
  - it lists pods to find the driver and reads its status and log;
  - on a failure it lists the pod's events and tries a non-fatal "already checked"
    pod patch;
  - cleanup deletes only the SparkApplication.

  So the Role also grants events (get, list, watch), which is read-only and puts the
  failure events in the task log. Pod patch isn't granted (denial is logged, not
  fatal), and neither is exec (only the XCom sidecar uses it).

### 6. `train-job.yaml` stays for rollback
Like the other pre-Operator Jobs, it stays under `deploy/app/jobs/`, with a comment
that `make train` no longer uses it.

## Risks / Trade-offs

- [The training DAG no longer seeds labels; a fresh environment fails `train` until
  someone runs `make labels`] → The failure is explicit and says what to run. The
  README's cluster run-order lists `make labels` before `make train`, and the proposal
  marks this as an operational breaking change.
- [The Spark Operator's pod RBAC needs vary by provider version] → Decision 5's
  verification is a real DAG run on minikube, plus `auth can-i` checks. Only the
  verbs actually needed are granted.
- [An extra executor pod for a small training set] → 1.5 GiB for the length of a
  run, within the headroom `make mk-check` already reserves for the largest stage.
- [The driver's `toPandas` of the sample now pulls rows from a remote executor] →
  The sample is bounded by `ml.train_max_rows` (REQ-ML-09), so the transfer and the
  driver's memory are too. The 2 GiB driver heap is unchanged.

## Migration Plan

1. `make image` (adds `pipelines/spark/train_job.py`), then `make spark-manifests`
   (adds `deploy/app/spark/train.yaml`).
2. `kubectl apply -f deploy/platform/airflow/rbac.yaml`. The removed Role and
   RoleBinding must be deleted explicitly (`kubectl delete role/rolebinding
   idp-pod-launcher -n idp-app`), since `apply` doesn't prune them.
3. `make airflow-reload` (new DAG and manifest).
4. Verify:
   - `make labels` (if needed), then `make train`: a `SparkApplication` completes, and
     MLflow shows a new run and versions;
   - trigger `idp_training` in Airflow: `train` completes and inference is triggered;
   - the `auth can-i` checks.
5. Rollback: restore the previous DAG and RBAC (`git revert`, then `make
   airflow-reload` and apply the RBAC). `train-job.yaml` still works for manual
   training.
