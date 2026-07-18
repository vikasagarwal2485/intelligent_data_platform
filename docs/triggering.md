# Triggering the Airflow DAG and the Spark pipeline

The medallion stages can be triggered three ways. Pick per situation:

| Trigger | Command | When |
|---|---|---|
| **On-demand job** | `make silver` / `make gold` / `make publish` | quickest; runs the `idp` image as a one-off K8s Job |
| **Airflow DAG** | `make airflow-trigger` (or the UI) | scheduled/orchestrated Silver → Gold → publish |
| **Spark jobs** | `make spark-silver` / `make spark-gold` | the distributed Silver/Gold engines (spark-submit) |

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

### Choosing the execution engine per stage (pandas vs Spark)

Airflow **orchestrates**; **Silver and Gold each have an independent execution
engine** — the DAG shape (`silver >> gold >> publish`) is identical either way:

- **`pandas`** (default) — the task launches `idp silver` / `idp gold` in the
  scoring-api image (single node).
- **`spark`** — the task launches `spark-submit` of `pipelines/spark/{silver,gold}_job.py`
  in an `apache/spark` pod (distributed). The Spark Silver job reproduces the
  **same per-tenant salted-SHA-256 tokenization**, so `service_id` tokens are
  identical whether Silver ran on pandas or Spark.

Select each independently with `idp_silver_engine` / `idp_gold_engine` (Airflow
Variables, or env `IDP_SILVER_ENGINE` / `IDP_GOLD_ENGINE`):

```bash
make spark-configmap                  # once: provide the Spark job scripts (idp-app)
make silver-engine ENGINE=spark       # Bronze->Silver on Spark
make gold-engine   ENGINE=spark       # Silver->Gold on Spark
make airflow-trigger
make silver-engine ENGINE=pandas      # switch either back independently
```

`publish` (Gold -> TimescaleDB) is always a **separate, independent task** — it is
never folded into Gold. (Next step: migrate its target to Iceberg via Nessie.)
Flip the variables in the Airflow UI (Admin -> Variables) too; effective on the
next DAG parse, no redeploy.

---

## B. Spark pipeline (production Silver / Gold)

`pipelines/spark/silver_job.py` reads Bronze and writes canonical Silver (with the
same PII tokenization as the pandas path); `pipelines/spark/gold_job.py` reads
Silver and writes Gold. Both use the S3A connector to MinIO. Run either as a
one-off Spark pod:

```bash
make spark-silver     # Bronze -> Silver on Spark
make spark-gold       # Silver -> Gold on Spark
```

Original one-liner (Gold shown; Silver is the same with `silver_job.py --salts ...`):

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
