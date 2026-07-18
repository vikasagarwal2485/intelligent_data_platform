# Triggering the Airflow DAG and the Spark pipeline

The medallion stages can be triggered three ways. Pick per situation:

| Trigger | Command | When |
|---|---|---|
| **On-demand job** | `make silver` / `make gold` / `make publish` | quickest; runs the `idp` image as a one-off K8s Job |
| **Airflow DAG** | `make airflow-trigger` (or the UI) | scheduled/orchestrated Silver → Gold → publish |
| **Spark job** | `make spark-gold` | the distributed Gold engine (spark-submit) |

The `idp_silver_gold` DAG runs `silver >> gold >> publish` — the last task
(`idp publish`) mirrors Gold into **TimescaleDB** for serving. On-demand, run
`make publish` after `make gold`. Inspect the serving tables with
`make timescale-psql`.

Prereqs for all: platform deployed (`make platform-services`), the app image built
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
- `helm upgrade ... -f deploy/platform/airflow/values.yaml` — installs Airflow with
  the DAGs **baked into a local image** (`deploy/platform/airflow/Dockerfile`; the
  source repo is private, so git-sync can't clone it).
- `kubectl apply -f deploy/platform/airflow/rbac.yaml` — grants the Airflow service
  account rights to launch pods in `idp-app`.

> Adjust the ServiceAccount names in `deploy/platform/airflow/rbac.yaml` if your
> chart differs: `kubectl -n idp-platform get sa | grep airflow`.

Confirm the DAGs are loaded:
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
- **`idp_silver_gold`** — `silver >> gold >> publish`, hourly.
- **`idp_train`** — offline model training, daily.

### Choosing the Gold execution engine (pandas vs Spark)

Airflow **orchestrates**; the Gold task's **execution engine** is a switch — the
DAG shape (`silver >> gold >> publish`) is identical either way:

- **`pandas`** (default) — the `gold` task launches `idp gold` in the scoring-api
  image (single node).
- **`spark`** — the `gold` task launches `spark-submit pipelines/spark/gold_job.py`
  in an `apache/spark` pod (distributed; for large data / backfills). It mounts the
  script from the `idp-gold-spark` ConfigMap.

Select it with the Airflow Variable `idp_gold_engine` (or env `IDP_GOLD_ENGINE`):

```bash
make spark-configmap                 # once: provide the Spark job script (idp-app)
make gold-engine ENGINE=spark        # sets the Airflow variable idp_gold_engine=spark
make airflow-trigger                 # gold now runs on Spark
make gold-engine ENGINE=pandas       # switch back to the single-node pandas engine
```

You can also flip `idp_gold_engine` in the Airflow UI (Admin → Variables). The
change takes effect on the next DAG parse; no redeploy.

---

## B. Spark pipeline (production Gold)

`pipelines/spark/gold_job.py` reads Silver and writes Gold on MinIO via the S3A
connector. `make spark-gold` runs it with `spark-submit` in a one-off Spark pod.

```bash
make spark-gold
```
This:
1. packages `gold_job.py` into a ConfigMap,
2. runs a Job (`deploy/app/jobs/spark-gold-job.yaml`) on the `apache/spark:3.5.3`
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
