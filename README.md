# Intelligent Data Platform

A telecom **data lakehouse** (medallion architecture) that ingests TM Forum
entity/event data, curates it, and serves two ML use cases:

1. **Alternative credit-limit scoring** — a probability-of-default model + limit sizing.
2. **Fraud detection** — supervised + anomaly + rules (SIM-box, recharge/card, roaming).

It is **multi-tenant, GDPR-aware and batch-first**, Kubernetes-native on both
tiers (local **minikube**, production **AWS EKS**). The full design is in
[`docs/architecture_and_implementation_plan.md`](docs/architecture_and_implementation_plan.md)
(reviewed in PR #1).

```
synthetic data ─▶ Bronze (raw) ─▶ Silver (canonical) ─▶ Features ─▶ ML ─▶ Gold + Scoring API
                                  Iceberg-style tables    Feast-style   credit / fraud
```

> **Prototype note.** The Python package (`src/idp`) implements the *canonical
> pipeline logic* with pandas + scikit-learn so the whole flow is unit-testable
> without a cluster. The production engines from the plan (Apache Kafka/Strimzi,
> Spark, Flink, Iceberg+Nessie, Trino, TimescaleDB, Keycloak+OPA, Airflow,
> MLflow) are deployed via [`deploy/`](deploy/) and documented below. The
> scikit-learn `HistGradientBoosting*` models are drop-in stand-ins for the
> LightGBM/XGBoost models named in the plan.

---

## Repository layout

```
src/idp/              # the platform package
  common/             #   config, multi-tenancy guards, GDPR PII tokenization
  synthetic/          #   synthetic data generator (Phase 0)
  ingestion/          #   Bronze (raw, immutable, tenant-partitioned)
  curation/           #   Silver (JSON:API unwrap, canonical dim/fact tables)
  features/           #   credit & fraud feature engineering
  ml/                 #   credit-limit model + fraud model
  serving/            #   FastAPI scoring service
  pipeline.py         #   end-to-end batch orchestration
  cli.py              #   `idp generate | run | serve`
tests/                # 60 tests (pytest) covering every layer
config/               # platform.yaml + Avro schema contracts
data/                 # generated local data (git-ignored); see data/README.md
data_samples/         # reference TM Forum JSON:API documents
pipelines/airflow/    # Airflow DAG for the batch pipeline
deploy/               # Kustomize base + minikube/AWS overlays + Argo CD
docs/                 # architecture & implementation plan
```

---

## Running the pipeline (Bronze → Silver → Gold) — two paths

There are **two ways** to run the medallion pipeline. They produce the same
Bronze/Silver/Gold data; pick based on whether you want to exercise the deployed
Kafka cluster.

| | **Path A — Batch (local)** | **Path B — Streaming (Kafka, in-cluster)** |
|---|---|---|
| Command | `make run` | `make stream` |
| Data path | synthetic → Bronze → Silver → Gold **in one process** | synthetic → **Kafka topics** → Bronze → Silver → Gold |
| Needs the cluster? | No (runs in the Python venv) | Yes (uses your deployed Kafka + MinIO) |
| Output | `data/lake/{bronze,silver,gold}/` (local) | MinIO bucket `idp-bronze` (prefixes `bronze/`,`silver/`,`gold/`) |
| ML training | Yes (labelled synthetic data) | No — produces Gold **feature tables**; scoring only if models exist |
| Use it for | fast dev, tests, model training | demonstrating the streaming architecture end-to-end |

> **Why two paths?** ML training needs the synthetic *labels* (fraud/default),
> which only exist in-process. The Kafka path streams the *entity/event* data
> (no labels), so it builds the Gold feature tables and applies already-trained
> models. Train with Path A, stream with Path B.

### Path A — batch (simplest, always works)
```bash
make run          # synthetic → Bronze → Silver → Gold + ML, into data/lake/
```

### Path B — streaming through your deployed Kafka
0. **Prereqs**: platform deployed (`make infra`), Kafka up with topics (`make kafka`).

1. **Rebuild the app image with the streaming deps** (kafka-python, s3fs) and
   load it into minikube:
   ```bash
   make image
   ```
2. **Produce + consume** (runs two Kubernetes Jobs in-cluster, so they use the
   internal Kafka DNS — external access from a Mac/minikube is unreliable):
   ```bash
   make stream
   ```
   - `idp-produce` publishes synthetic records to `t.<tenant>.<entity>` topics.
   - `idp-consume` drains the topics → Bronze → Silver → Gold, written to MinIO.
3. **Watch / verify**:
   ```bash
   kubectl -n idp-platform logs -f job/idp-produce      # per-topic message counts
   kubectl -n idp-platform logs -f job/idp-consume      # bronze/silver/gold counts
   ```
   - In **Kafka UI** (`make kafka-ui`) you'll see messages in the topics.
   - In the **MinIO console** you'll see `idp-bronze/{bronze,silver,gold}/…`.

> Tip: the default synthetic volume (~1M usage events) is a lot to stream on a
> small node. For a quicker demo, lower `synthetic.subscribers_per_tenant` and
> `synthetic.days` in `config/platform.yaml` before `make stream`.

---

## Quick start (local Python)

Requires Python 3.10+.

```bash
make install      # create .venv and install the package + dev deps
make test         # run the full test suite (~30s)
make run          # generate data + run the full pipeline, writing to data/lake/
make serve        # start the scoring API on http://localhost:8000
```

`make run` prints a summary like:

```json
{
  "counts": {"subscribers": 1000, "usage_events": 1000000, "recharge_events": 31000},
  "credit_metrics": {"auc": 0.72, "ks": 0.36, "gini": 0.44},
  "fraud_metrics":  {"pr_auc": 0.68, "precision_at_20": 1.0}
}
```

(Out-of-sample metrics on the synthetic default config; deterministic given the
seed. Credit AUC ≈ 0.72 and fraud precision@20 = 1.0 — the worst offenders are
ranked perfectly — are realistic, non-overfit numbers.)

### Try the scoring API
```bash
curl -s localhost:8000/health
curl -s -X POST localhost:8000/credit-limit -H 'content-type: application/json' \
  -d '{"tenant_id":"tenant_a","service_id":"tok_demo","features":{"recharge_avg_amount":40,"account_tenure_days":500,"recharge_success_rate":0.95}}'
curl -s -X POST localhost:8000/fraud-score -H 'content-type: application/json' \
  -d '{"tenant_id":"tenant_a","service_id":"tok_demo","features":{"originating_ratio":0.95,"calls_per_active_day":250}}'
```

### CLI
```bash
idp generate                 # write synthetic landing files to data/landing/
idp run                      # Bronze->Silver->Features->ML->Gold (writes data/lake/)
idp run --no-persist         # in-memory smoke run
idp serve --port 8000        # scoring API
idp --config path.yaml run   # use a custom config
```

Configuration (tenants, volumes, model policy) lives in
[`config/platform.yaml`](config/platform.yaml).

---

## Step-by-step deployment

### A. Local Kubernetes (minikube)

Requires `minikube`, `kubectl`, `kustomize`, `helm`.

1. **Start the cluster**
   ```bash
   make mk-up    # minikube start with ingress, metrics-server, storage-provisioner
   ```
2. **Build the scoring-api image into minikube's Docker daemon** (local, no registry)
   ```bash
   make image    # eval $(minikube docker-env) && docker build -t idp/scoring-api:dev .
   ```
   The local overlay sets `imagePullPolicy: Never`, so Kubernetes uses this
   locally-built image instead of pulling `idp/scoring-api:dev` from a registry
   (which would fail with `ImagePullBackOff`).
3. **Deploy platform + per-tenant namespaces (RBAC, quotas, NetworkPolicy) + scoring API**
   ```bash
   make deploy        # kubectl apply -k deploy/overlays/local
   ```
   The overlay also deploys the scoring API (from the image built in step 2).
4. **Install platform infrastructure** into `idp-platform`:
   ```bash
   make infra        # bash deploy/install-infra.sh idp-platform
   ```
   This **adds the required Helm repositories first** (MinIO, Strimzi, Nessie,
   Trino, Airflow, MLflow, Prometheus) — the step whose absence causes
   `Error: repo <x> not found` — then installs each component. It deliberately
   **avoids Bitnami charts** (whose free images were removed on 2025-08-28,
   causing `ImagePullBackOff` on `bitnami/minio:...`): MinIO uses its official
   chart, and TimescaleDB/Keycloak use plain manifests with upstream images
   (`timescale/timescaledb`, `quay.io/keycloak`). The script is idempotent and
   first removes any earlier broken Bitnami releases. See
   [`deploy/README.md`](deploy/README.md) for the full source table.
5. **Deploy Kafka, create topics + register Avro schemas**
   ```bash
   make kafka        # bash deploy/kafka/setup-kafka.sh idp-platform
   ```
   Deploys a Strimzi Kafka cluster + Apicurio Schema Registry, creates the
   per-tenant `t.<tenant>.*` topics, and registers the Avro contracts from
   [`config/avro/`](config/avro) as `<topic>-value` subjects. Full step-by-step
   (and the imperative CLI alternative) is in
   [`deploy/kafka/README.md`](deploy/kafka/README.md).
6. **Initialize Nessie/Iceberg namespaces** (`silver`, `gold`) via Trino/Spark.
7. **Load the Airflow DAG**: mount `pipelines/airflow/dags/idp_batch_dag.py` into
   the Airflow `dags/` folder; it schedules the daily batch run.
8. **Run the pipeline**: see
   [Running the pipeline (Bronze → Silver → Gold)](#running-the-pipeline-bronze--silver--gold)
   above — `make stream` to flow synthetic data through Kafka into the lake, or
   `make run` for the local batch pipeline. Quick in-venv smoke check:
   ```bash
   make smoke    # ingest -> Bronze -> Silver -> feature -> score (asserts isolation)
   ```

### B. Production (AWS EKS)

1. Provision EKS + ECR + S3 (per-tenant prefixes & KMS) + IRSA.
2. Build and push the image to ECR (via GitHub Actions, see `.github/workflows/`).
3. Deploy with the AWS overlay, or let **Argo CD** sync it:
   ```bash
   kubectl apply -k deploy/overlays/aws
   kubectl apply -f deploy/argocd/application.yaml
   ```
4. Wire secrets via **External Secrets + AWS Secrets Manager/KMS**, ALB ingress,
   and per-tenant data residency (region pinning).

See [`deploy/README.md`](deploy/README.md) for overlay details.

---

## How it maps to the plan

| Plan section | Implementation |
|---|---|
| 1.5 Multi-tenancy / RBAC / GDPR | `common/tenancy.py`, `common/pii.py`, `deploy/base/{rbac,isolation}.yaml` |
| 3.0 Synthetic data | `synthetic/generator.py` (coherent, labelled, multi-tenant) |
| 3.2 Bronze ingestion | `ingestion/bronze.py` (tenant-partitioned Parquet) |
| 3.3 Silver curation | `curation/silver.py`, `curation/jsonapi.py` (JSON:API unwrap, PII tokenization) |
| 4 Feature engineering | `features/credit_features.py`, `features/fraud_features.py` |
| 5 Credit-limit ML | `ml/credit_model.py` (heuristic + PD model + limit sizing + scorecard) |
| 6 Fraud detection | `ml/fraud_model.py` (rules + gradient boosting + IsolationForest fusion) |
| 5.6 / 6.6 Serving | `serving/api.py` (FastAPI), `deploy/base/scoring-api.yaml` |
| Q5 Orchestration | `pipelines/airflow/dags/idp_batch_dag.py` |

---

## Testing

```bash
make test        # or: .venv/bin/python -m pytest
```
60 tests cover: synthetic generation & referential integrity, tenant isolation,
GDPR tokenization, JSON:API unwrap against the real `data_samples/`, ingestion
round-trip, Silver curation, feature engineering, both ML models (signal +
persistence), the end-to-end pipeline, and the scoring API.

---

## Status

All six implementation phases from the plan are in place (Phase 5 real-time
Flink scoring is intentionally deferred per the review — batch-first). The
prototype runs end-to-end locally and the Kubernetes deployment artifacts are
provided for minikube and EKS.
