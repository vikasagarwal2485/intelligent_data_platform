# Proposal

## Why

Silver and Gold run on Spark, but only as `local[*]` inside a single pod. One
container's memory bounds each stage, so the data size a stage can handle is capped
by one node, and nothing scales out. Memory is also easy to misjudge. Silver pods
were OOMKilled far below their 3 Gi limit. `iceberg-incremental-lakehouse` found
why: the minikube node is a Docker container capped at 6 GB, while Kubernetes saw
21 GB. So the node-level OOM killer took the largest process, the Spark JVM. A solo
Silver pod peaks at about 1.2 GB. The cap was raised to 12 GB. Nothing yet checks
that the stages' memory profiles fit the node's real memory.
(`max_active_runs=1` on the features DAG stays for its own reason: only one
streaming query may be active per Silver checkpoint.)

Three stages still process whole tables in one Python process:
- `idp score` loads every Gold feature row into pandas.
- `idp publish` writes every serving row through pandas `to_sql`.
- `idp train` reads the full labelled feature set.

For a Tier-1 operator (50 M subscribers) none of these fit in one pod.

This is change 3 of the lakehouse roadmap. It requires `spark-single-engine`
(archived) and `iceberg-incremental-lakehouse` (Iceberg I/O and catalog config).

## What Changes

- **The Kubeflow Spark Operator** is installed in `idp-platform` and watches
  `idp-app`. The Silver, Gold, Score, publish and maintenance stages run in the
  cluster as `SparkApplication`s: one driver plus executors, all from the same `idp`
  image, running the same `idp` stage code.
  - Local and CI keep `local[*]`; only the master and resources differ (REQ-ORC-05).
  - The non-Spark stages (produce, consume, labels, train) stay Kubernetes Jobs.
- **Explicit, per-stage resource profiles** in config: driver and executor cores,
  memory, `memoryOverhead`, and executor count or dynamic-allocation bounds.
  - Every pod's limit covers heap plus overhead, so a container OOM is prevented by
    configuration, not by serializing runs.
  - A preflight check (`make mk-check`) fails when the node's real memory cap is
    below what Kubernetes reports, or below the default profiles plus the
    platform's footprint.
- **Distributed scoring.** Score reads the Gold features as a Spark DataFrame,
  broadcasts the trained models, and scores each partition with `mapInPandas`.
  pandas is used per batch, never on a whole table. It appends the score run to
  Iceberg.
- **Distributed publish.** publish reads the latest score run and the features with
  Spark and writes them to TimescaleDB over JDBC into staging tables. It then swaps
  each staging table into place in a single transaction under the existing advisory
  lock, keeping the unique `(tenant_id, service_id)` key. Readers still see either
  the old or the new table.
- **Bounded training.** `idp train` reads at most `ml.train_max_rows` labelled rows,
  as a stratified sample per tenant and label, so training memory doesn't grow with
  the subscriber base.
- **Airflow** submits the Spark stages as `SparkApplication`s
  (`SparkKubernetesOperator`) and waits for them to finish. The DAG shapes, datasets
  and `max_active_runs` are unchanged.

Non-goals:
- Autoscaling the Kubernetes node pool, and Spark on EKS/EMR specifics. The design
  notes the EKS path only.
- Streaming execution.
- GPU training.
- The MCP read path (change 4).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `orchestration`:
  - REQ-ORC-01: the Spark stages run as `SparkApplication`s in the cluster.
  - REQ-ORC-02: DAG tasks submit `SparkApplication`s.
  - REQ-ORC-05: the master is `local[*]` or Kubernetes through the operator; pandas
    is limited to per-partition scoring, bounded training and the Python landing.
  - New REQ-ORC-07: per-stage resource profiles and scale-out.
- `ml-models`: REQ-ML-07 (distributed per-partition scoring); new REQ-ML-09 (bounded
  training sample).
- `serving-api`: REQ-SRV-01 (Spark-read publish with atomic staging-table swap).
- `platform-deployment`: new REQ-PLT-10 (Spark Operator).

## Impact

- **Code:**
  - `src/idp/transforms/session.py` (profiles, Kubernetes master settings)
  - `src/idp/pipeline.py` (`to_scores`, `publish_serving`, `train` sampling)
  - `src/idp/serving/timescale.py` (staging swap)
  - new `src/idp/transforms/scoring.py` (`mapInPandas` scoring)
  - `pipelines/spark/*_job.py` (entrypoints for all Spark stages)
  - `config/platform.yaml` (`spark.profiles`, `ml.train_max_rows`)
- **Deploy:**
  - Spark Operator Helm install (`install-infra.sh`, `make spark-operator`)
  - `deploy/app/spark/*.yaml` `SparkApplication` templates
  - driver ServiceAccount and RBAC in `idp-app`
  - Airflow RBAC to create `sparkapplications`
  - the DAG switches to `SparkKubernetesOperator` for the Spark stages
  - the image already bundles the PostgreSQL JDBC driver (it is the lake catalog's
    driver since `iceberg-incremental-lakehouse`); publish reuses it
- **Tests:** local Spark as before. New tests for scoring parity (distributed vs
  in-process scores are identical), the staging swap, and sampling.
- **Ordering:** after `iceberg-incremental-lakehouse`. The in-flight
  `mlflow-model-registry` should land before or after as a unit, because both change
  how Score loads models; the design notes the seam.
