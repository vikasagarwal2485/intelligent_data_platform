# Project Context

## Mission

Build a **multi-tenant telecom data lakehouse** that ingests TM Forum
entity/event data through a **medallion architecture (Bronze → Silver → Gold)**
and serves two ML decisioning products: **alternative credit-limit scoring** and
**fraud detection**. The platform is GDPR-aware, tenant-isolated, batch-first,
and Kubernetes-native (local minikube, production AWS EKS).

## Source data (ground truth)

Reference records live in [`data_samples/`](../data_samples/): one TM Forum Open
API v5 resource per landing file type (Individual, Organization, Customer,
BillingAccount, Agreement, Product, AppliedCustomerBillingRate, TopupBalance,
ProductUsage). Silver maps their events to the canonical entities (`parties`,
`customers`, `billing_accounts`, `agreements`, `products`, voice/data usage,
recharges, fees) with the profile in `config/tmf/profiles/`; everything downstream
uses the canonical model.

## Tech stack & standing choices (see decisions/decision-log.md)

| Concern | Choice |
|---|---|
| Language / engines | Python 3.11 and JDK 17 baseline; Spark for Silver/Gold, pandas/scikit-learn for bounded ML |
| Lake | Apache Iceberg for Bronze/Silver/Gold; Flink real-time execution is deferred |
| Object store | MinIO (local) / AWS S3 (prod); one bucket per layer: `idp-bronze`/`idp-silver`/`idp-gold` |
| Streaming | Apache Kafka (open source, Strimzi operator) + Apicurio Schema Registry (TM Forum JSON Schema wire contracts) |
| Catalog | SQL/JDBC Iceberg catalog: SQLite locally, PostgreSQL in the cluster; Nessie provisioned but not active |
| Query | Trino |
| Serving DB | TimescaleDB |
| Identity / RBAC | Keycloak OIDC for scoring API tenant claims; Trino read-only analytics identity; Kubernetes RBAC |
| Orchestration | Apache Airflow |
| Experiment/registry | MLflow |
| Observability | Prometheus + Grafana |
| Serving API | FastAPI |
| ML libs | scikit-learn `HistGradientBoosting*` (stands in for LightGBM/XGBoost), IsolationForest |
| K8s | minikube (local) / EKS (prod); Kustomize base + overlays; Argo CD; Helm for services |

## Repository conventions

- Package lives in `src/idp/` (installed via `pip install -e .`). Sub-packages:
  `common` (config, tenancy, pii, storage), `synthetic`, `ingestion`,
  `transforms` (Spark Silver/Gold/score/publish), `ml`, `streaming`, `serving`,
  `mcp`; plus `pipeline.py`
  (stage functions) and `cli.py` (`idp` entrypoint).
- Every dataset carries a leading `tenant_id` partition column — the isolation
  boundary. All lake reads must be tenant-scoped.
- PII (MSISDN/IMSI/IMEI/phone/email/other-party) is tokenized with a
  **per-tenant salted SHA-256** before leaving Silver; tokens are deterministic
  per tenant so they still join.
- Each medallion stage is an **independent function** that reads its input layer
  and writes its output layer — no cross-stage chaining inside one job.
  `to_bronze` / `to_silver` / `to_gold` / `train` / `to_scores` / `publish_serving`;
  `run()` only *composes* them
  locally for dev/testing.
- Config is `config/platform.yaml` with env overrides (`IDP_*`); S3 vs local is
  driven by `IDP_S3_ENDPOINT`.

## Commands

```bash
make install PYTHON=python3.11  # venv + editable install + dev deps (JDK 17)
make check        # workspace/config/dependency checks
make test         # pytest (must stay green after every phase)
make run          # local pipeline: generate -> bronze -> silver -> labels -> gold -> train -> score
.venv/bin/idp --help            # individual stage commands
```

`idp publish` materializes Gold into TimescaleDB (`gold.*`, indexed by
`(tenant_id, service_id)`) for low-latency serving.

Deployment: `make mk-up`, `make image`, `kubectl apply -k deploy/platform/overlays/local`,
`make platform-services`, `make kafka`, then `make stream|silver|gold`. See root README §3.

## Definition of done (per phase)

1. All tasks in the phase's `tasks.md` are checked.
2. Every scenario in the capability specs the phase touches is satisfiable.
3. `make test` is green; new behavior has tests.
4. No current decision in `decisions/decision-log.md` is violated.

Phases 0–15 are archived under `openspec/changes/archive/`; use the
[specification guide](README.md) for the current change workflow and
[operator runbook](../internal/runbook.md) for setup and verification details.
