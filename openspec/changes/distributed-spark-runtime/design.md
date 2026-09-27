# Design

## Context

- **After `iceberg-incremental-lakehouse`:**
  - Every Spark stage builds its session with `get_spark(cfg)` (`local[*]`, fixed
    driver memory, the Iceberg `lake` catalog).
  - Jobs run as single-pod Kubernetes Jobs (`deploy/app/jobs/*.yaml`) or as Airflow
    `KubernetesPodOperator` tasks (request 1.5 Gi, limit 3 Gi, `IDP_SPARK_DRIVER_MEMORY=1g`).
- **Three stages still run whole tables through one Python process:**
  - `to_scores` reads all Gold features into pandas and calls
    `CreditLimitModel.recommend_limit` and `FraudModel.score`.
  - `publish_serving` loads everything into pandas and calls `to_sql` under a
    Postgres advisory lock.
  - `train` reads the full Gold-plus-labels join.
- **The cluster:** minikube with 8 CPUs; Kubernetes reports about 20 Gi, but the node
  container's real cap is 12 GB, and the platform services use about 5.5 GB. Airflow 2.9.3 with the
  `cncf.kubernetes` provider, which includes `SparkKubernetesOperator`.
- **The lake** is Iceberg in a JDBC catalog on the platform Postgres (database
  `iceberg`, ADR-0028). Only the driver talks to the catalog; executors get the
  table's FileIO (S3FileIO) with the plan. Incremental Silver is Structured
  Streaming with `Trigger.Once` and a `foreachBatch` whose `MERGE` runs as a normal
  distributed job. The checkpoints are on `s3a://idp-silver/_checkpoints`.
- **The earlier Silver OOM is explained (not a pod limit):** the minikube node is a
  Docker container that was capped at 6 GB while Kubernetes saw about 21 GB. A solo
  Silver pod's cgroup peaked at about 1.2 GB when the node-level OOM killer took its
  JVM. The cap is now 12 GB (`docker update`). Pod-level accounting was never the
  problem; node-level headroom was.

## Goals / Non-Goals

**Goals:**
- Spark stages run as driver plus executors in the cluster; the code and local mode
  stay unchanged.
- Container memory is fully specified (heap plus overhead) per stage profile.
- Scoring and publish scale out; training memory is bounded.

**Non-Goals:**
- Node autoscaling. Spark dynamic allocation needs shuffle tracking on Kubernetes,
  which is off by default; fixed executor counts come first.
- A Spark History Server. An event log to MinIO is enough for now.
- Replacing TimescaleDB as the serving store.

## Decisions

### 1. Kubeflow Spark Operator, with SparkApplication templates rendered from config
- **Install:** the Helm chart `spark-operator/spark-operator` (v2.x) in
  `idp-platform`, with `spark.jobNamespaces=[idp-app]` and webhook enabled (for volume
  and env injection).
- **Per stage, `deploy/app/spark/<stage>.yaml`** is a `SparkApplication`:
  - `type: Python`, `pythonVersion: "3"`
  - `mode: cluster`, `image: idp/scoring-api:dev`
  - `mainApplicationFile: local:///app/pipelines/spark/<stage>_job.py`: the image
    now also copies `pipelines/spark`, and the entrypoints stay the thin wrappers
    over `idp.pipeline`
  - driver and executor specs from the stage profile
  - env: catalog, S3 and TimescaleDB for publish
- **Rendering:** `idp spark-manifest <stage>` renders the manifest from
  `config/platform.yaml` `spark.profiles.<stage>`, so profiles live in one place.
  `make silver` and friends apply the rendered manifest and wait for
  `.status.applicationState.state` to be `COMPLETED` or `FAILED`. The rendered
  templates are also committed, for GitOps and review.
- *Alternative:* a raw `spark-submit --master k8s://`. Rejected: no CRD status for
  Airflow to watch, and retries and cleanup would have to be hand-built.

### 2. Profiles and memory accounting
`spark.profiles.<stage>` holds:
- `driver: {cores, memory, memoryOverhead}`
- `executor: {instances, cores, memory, memoryOverhead}`

The pod limit is heap plus overhead; the operator sets it from these values.

Defaults for minikube (8 CPUs, 20 Gi):

| Stage | Driver | Executors |
|---|---|---|
| silver, gold | 1g + 512m | 2 × 2g + 512m |
| score | 1g + 512m | 2 × 2g + 768m (Python workers run pandas) |
| publish, maintain | 1g + 512m | 1–2 executors |

- In `local[*]` mode (tests, `idp run`), `get_spark` ignores the executor settings.
- **Node headroom check** (task 1.2): `make mk-check` compares the minikube node
  container's cap (`docker inspect minikube`) with the node's reported memory and
  with the largest stage's total pod memory (driver plus executors, heap plus
  overhead) plus the platform footprint. It fails with the `docker update` fix when
  the cap is too small, so a node-level OOM can't masquerade as a pod OOM again. The
  overhead defaults start from the measured solo peak (about 1.2 GB for a whole
  Silver in one pod) and are confirmed by the scale check (task 6.3).

### 3. Distributed scoring with `mapInPandas`
`transforms/scoring.py`:
1. The driver loads each model from the store (or from the MLflow registry once
   that change lands; the loader is one function, `load_scoring_models(cfg)`, which
   is the seam between the two changes).
2. It broadcasts the models with `sc.broadcast`.
3. `features_df.mapInPandas(score_batch, schema)` calls `model.recommend_limit(batch)`
   or `model.score(batch)` on each Arrow batch and adds `scored_at`, a single driver
   timestamp passed in.
4. The run is appended to `gold.credit_scores` / `gold.fraud_scores` through the
   Spark Iceberg writer.

`FraudModel.score` normalizes the anomaly score with a min–max over the frame it
receives. Per partition, that would give different values than over the whole
table. The fix is to compute the population min and max of the raw IsolationForest
score once, in a pass with `mapInPandas` plus a Spark aggregate, and pass them in.
`FraudModel` gains `score(X, anomaly_range=None)`: `None` keeps today's single-frame
behaviour, and distributed scoring passes the global range. SCN-ML-07.5 pins this.
- *Alternative:* a pandas UDF per column. Rejected, because the models score whole
  rows and produce several outputs.

### 4. Publish: Spark JDBC into staging, then an atomic swap
For each serving table:
1. Spark computes the latest run per tenant (`scored_at = max over tenant`) for the
   score tables.
2. `df.write.jdbc(url, "gold.<t>__staging", mode="overwrite")` with
   `batchsize`, `numPartitions` and `truncate` set.
3. The driver opens one psycopg transaction:
   `pg_advisory_xact_lock(hashtext('gold.<t>'))` →
   `CREATE UNIQUE INDEX ux_<t>__staging_key ON gold.<t>__staging(tenant_id, service_id)`
   → `DROP TABLE IF EXISTS gold.<t>` → `ALTER TABLE gold.<t>__staging RENAME TO <t>`
   → rename the index → `COMMIT`.

A duplicate key fails the index build, so the old table survives (REQ-SRV-05). The
PostgreSQL JDBC driver is already in the image (the lake catalog uses it). `timescale.publish_frame` stays for
the SQLite-backed unit tests of the swap logic, which is factored as
`swap_in(conn, table)`.

### 5. Bounded training
`train` reads the labels (small, Parquet) and the Gold features with a Spark job. The
job joins them, samples with `sampleBy` over a `tenant_id || label` stratum key
(fixed seed), caps the result at `ml.train_max_rows` (default 2 000 000), and only
then calls `toPandas()`. The metrics gain `sampled_rows` and `sample_seed`. Train
keeps running as a Kubernetes Job with a local Spark session sized by the train
profile: sampling needs Spark, fitting doesn't.

### 6. Airflow
- **Spark tasks** use `SparkKubernetesOperator(application_file=<rendered manifest>,
  namespace="idp-app", kubernetes_conn_id="kubernetes_default",
  delete_on_termination=True)`, and wait for the final state.
- **The rendered manifests** are baked into the DAG image next to the DAG file.
- **Unchanged:** `max_active_runs=1` on features, and the datasets.
- **Airflow RBAC:** a `sparkapplications` role in `idp-app`.

## Risks / Trade-offs

- **Operator webhook and cert-manager requirements** differ between chart versions.
  → The chart version is pinned. The webhook is only needed for pod customizations;
  the design uses only driver/executor env and resources, which work without it. It
  can be disabled if it causes install issues.
- **Node memory cap.** One Spark stage in the cluster (e.g. Silver: 1.5 GB driver
  plus 2 × 2.5 GB executors) and the platform (about 5.5 GB) together approach the
  12 GB cap. → `make mk-check` runs before the stage targets, and the concurrency
  check (SCN-ORC-07.3) sizes `maintain` with one small executor.
- **Executor pods on a single minikube node** don't really scale. → The goal here is
  correctness plus configuration. The scale-out scenario (SCN-ORC-07.2) is verified
  with 10× synthetic data within node capacity, and the EKS path is documented.
- **Scoring parity with a global anomaly range** changes `FraudModel.score`'s
  signature. → A backward-compatible default, and a test that in-process scores are
  unchanged.
- **JDBC load time for large serving tables.** → Parallel partitions and batched
  inserts. The swap keeps readers unaffected during the load.
- **Interplay with `mlflow-model-registry`**, which also changes model loading. →
  `load_scoring_models` is the single seam. Whichever change lands second rebases
  onto it.

## Migration Plan

1. `make spark-operator`, then `make image` (includes `pipelines/spark` and the JDBC
   driver), then apply the Spark RBAC, then `make airflow-reload`.
2. `make silver gold score publish`, which now runs as Spark applications. Verify the
   results equal the previous run (counts, spot values) and that executor pods ran.
3. Trigger `idp_features`, and verify that `idp_inference` succeeds.
4. Rollback: the previous DAG image and `make` targets use the single-pod Jobs, which
   are still in `deploy/app/jobs`. They are removed only after one stable week.
