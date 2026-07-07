# Triggering the Airflow DAG and the Spark pipeline

The medallion stages can be triggered three ways. Pick per situation:

| Trigger | Command | When |
|---|---|---|
| **On-demand job** | `make silver` / `make gold` | quickest; runs the `idp` image as a one-off K8s Job |
| **Airflow DAG** | `make airflow-trigger` (or the UI) | scheduled/orchestrated Silver → Gold |
| **Spark job** | `make spark-gold` | the distributed Gold engine (spark-submit) |

Prereqs for all: platform deployed (`make infra`), the app image built
(`make image`), and Bronze already populated (`make stream`) so Silver has input.

---

## A. Airflow DAG (production Silver → Gold)

The DAG tasks launch **pods that run the `idp` image** (`idp silver`, `idp gold`)
via `KubernetesPodOperator`, so Airflow itself doesn't need the `idp` package —
only permission to create pods in `idp-app`.

### 1. Wire it up (one time)
```bash
make airflow-wire
```
This does two things:
- `helm upgrade ... -f deploy/airflow/values.yaml` — enables **git-sync** so the
  scheduler loads DAGs from `pipelines/airflow/dags/` in the repo.
- `kubectl apply -f deploy/airflow/rbac.yaml` — grants the Airflow service
  account rights to launch pods in `idp-app`.

> Adjust the ServiceAccount names in `deploy/airflow/rbac.yaml` if your chart
> differs: `kubectl -n idp-platform get sa | grep airflow`.

Wait for git-sync to pick up the DAG (up to ~1 min), then confirm it's loaded:
```bash
kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags list | grep idp
```

### 2. Trigger it
**CLI (fastest):**
```bash
make airflow-trigger      # unpauses + triggers idp_silver_gold now
```
**or the Web UI:**
```bash
make airflow-ui           # http://localhost:8080  (default admin/admin)
```
In the UI: find **`idp_silver_gold`**, toggle it **On** (unpause), then click
**Trigger DAG** (▶). Watch `silver` → `gold` run; open each task's **Logs**.

### 3. Verify output
```bash
make minio-ls             # Silver in idp-silver, Gold in idp-gold
```

DAGs provided (`pipelines/airflow/dags/idp_medallion_dag.py`):
- **`idp_silver_gold`** — `silver >> gold`, hourly.
- **`idp_train`** — offline model training, daily.

---

## B. Spark pipeline (production Gold)

`pipelines/spark/gold_job.py` reads Silver and writes Gold on MinIO via the S3A
connector. `make spark-gold` runs it with `spark-submit` in a one-off Spark pod.

```bash
make spark-gold
```
This:
1. packages `gold_job.py` into a ConfigMap,
2. runs a Job (`deploy/jobs/spark-gold-job.yaml`) on the `apache/spark:3.5.3`
   image in `local[*]` mode with the S3A config pointed at MinIO,
3. waits and prints the driver logs.

Verify:
```bash
kubectl -n idp-app logs job/idp-spark-gold      # Spark driver output
make minio-ls                                   # gold tables in idp-gold
```

**Notes / caveats**
- The job fetches `hadoop-aws:3.3.4` via `--packages` on first run, so it needs
  egress; `hadoop-aws` must match the image's Hadoop version (3.3.4 for Spark 3.5.x).
- `local[*]` runs a single-pod Spark. For a real cluster use the **Spark Operator**
  (`SparkApplication` CR) or `--master k8s://…`; the same `gold_job.py` applies.
- On EKS the target reads/writes **Iceberg** via the Nessie catalog instead of raw
  Parquet — the job structure is unchanged.

---

## Which should I use?

- **Developing / a quick refresh:** `make silver` / `make gold` (the `idp` Jobs).
- **Scheduled, dependency-managed runs:** the **Airflow** DAG.
- **Scale-out Gold on large Silver:** the **Spark** job.

All three run the *same* medallion logic; they differ only in the runtime/engine.
