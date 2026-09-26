# Triggering the Airflow DAG and the Spark pipeline

The medallion stages can be triggered three ways. Pick per situation:

| Trigger | Command | When |
|---|---|---|
| **On-demand job** | `make silver` / `make gold` / `make publish` | quickest; runs the `idp` image as a one-off K8s Job |
| **Airflow DAG** | `make airflow-trigger` (or the UI) | scheduled/orchestrated Silver → Gold → publish |
| **spark-submit** | `spark-submit pipelines/spark/{silver,gold}_job.py` | a future Spark cluster / Spark Operator (same code) |

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

### One engine for Silver and Gold (Spark)

Silver and Gold always run on **Spark** — one implementation (`src/idp/transforms`)
shared by `make silver` / `make gold`, the Airflow tasks, `idp run` and the tests
(ADR-0027). Each stage pod runs the `idp` image with Spark as `local[*]` inside the
pod, a fixed driver heap (`IDP_SPARK_DRIVER_MEMORY`, default `1g`) and Spark-sized
pod resources (request 1.5 Gi / limit 3 Gi). The image bundles the JRE, pyspark
and the S3A jars, so nothing is downloaded at run time. There is no engine
switch; the former `idp_silver_engine` / `idp_gold_engine` Variables have no effect
and can be deleted.

`publish` (Gold -> TimescaleDB) is always a **separate, independent task** — it is
never folded into Gold. (Next step: migrate its target to Iceberg via Nessie.)

---

## B. spark-submit entrypoints (future Spark cluster)

`pipelines/spark/silver_job.py` and `pipelines/spark/gold_job.py` are thin wrappers
over the same `idp` stages (`idp.pipeline.to_silver` / `to_gold`), for running on a
real Spark cluster later (Spark Operator `SparkApplication`, or
`--master k8s://…`). They need the `idp` package in the Spark runtime — the `idp`
image already has it:

```bash
spark-submit pipelines/spark/silver_job.py [--config config/platform.yaml]
spark-submit pipelines/spark/gold_job.py   [--config config/platform.yaml]
```

Lake location, tenant salts, lookback windows and the Spark master/memory come
from `config/platform.yaml` + `IDP_*` env, exactly as for `idp silver` / `idp gold`.
On EKS the target reads/writes **Iceberg** via the Nessie catalog instead of raw
Parquet (roadmap) — the stage code is unchanged.

---

## Which should I use?

- **Developing / a quick refresh:** `make silver` / `make gold` (the `idp` Jobs).
- **Scheduled, dependency-managed runs:** the **Airflow** DAG.
- **Scale-out on large data:** the same stages on a Spark cluster (roadmap:
  distributed Spark runtime) — set `IDP_SPARK_MASTER`, no code change.

All of them run the *same* Spark code; they differ only in the trigger and the
Spark master/resources.
