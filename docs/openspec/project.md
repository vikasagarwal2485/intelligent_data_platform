# Project Context

## Mission

Build a **multi-tenant telecom data lakehouse** that ingests TM Forum
entity/event data through a **medallion architecture (Bronze → Silver → Gold)**
and serves two ML decisioning products: **alternative credit-limit scoring** and
**fraud detection**. The platform is GDPR-aware, tenant-isolated, batch-first,
and Kubernetes-native (local minikube, production AWS EKS).

## Source data (ground truth)

Reference records live in [`data_samples/`](../../data_samples): TM Forum
JSON:API documents for `parties`, `accounts`, `agreements`, `products`,
`voice_usage_events`, `data_usage_events`, `recharge_events`. All downstream
schemas derive from these.

## Tech stack & standing choices (see decisions/decision-log.md)

| Concern | Choice |
|---|---|
| Language / prototype engine | Python 3.11, pandas, scikit-learn (unit-testable without a cluster) |
| Production engines (target) | Spark (batch), Flink (real-time, deferred), Apache Iceberg |
| Object store | MinIO (local) / AWS S3 (prod); one bucket per layer: `idp-bronze`/`idp-silver`/`idp-gold` |
| Streaming | Apache Kafka (open source, Strimzi operator) + Apicurio Schema Registry (Avro) |
| Catalog | Nessie (Iceberg) |
| Query | Trino |
| Serving DB | TimescaleDB |
| Identity / RBAC | Keycloak (OIDC) + OPA |
| Orchestration | Apache Airflow |
| Experiment/registry | MLflow |
| Observability | Prometheus + Grafana |
| Serving API | FastAPI |
| ML libs | scikit-learn `HistGradientBoosting*` (stands in for LightGBM/XGBoost), IsolationForest |
| K8s | minikube (local) / EKS (prod); Kustomize base + overlays; Argo CD; Helm for services |

## Repository conventions

- Package lives in `src/idp/` (installed via `pip install -e .`). Sub-packages:
  `common` (config, tenancy, pii, storage), `synthetic`, `ingestion`,
  `curation`, `features`, `ml`, `streaming`, `serving`; plus `pipeline.py`
  (stage functions) and `cli.py` (`idp` entrypoint).
- Every dataset carries a leading `tenant_id` partition column — the isolation
  boundary. All lake reads must be tenant-scoped.
- PII (MSISDN/IMSI/IMEI/phone/email/other-party) is tokenized with a
  **per-tenant salted SHA-256** before leaving Silver; tokens are deterministic
  per tenant so they still join.
- Each medallion stage is an **independent function** that reads its input layer
  and writes its output layer — no cross-stage chaining inside one job.
  `to_bronze` / `to_silver` / `to_gold` / `train`; `run()` only *composes* them
  locally for dev/testing.
- Config is `config/platform.yaml` with env overrides (`IDP_*`); S3 vs local is
  driven by `IDP_S3_ENDPOINT`.

## Commands

```bash
make install      # venv + editable install + dev deps
make test         # pytest (must stay green after every phase)
make run          # local compose: generate -> bronze -> silver -> train -> gold
idp produce|consume|silver|gold|train|serve   # individual stages
```

Deployment: `make mk-up`, `make image`, `kubectl apply -k deploy/overlays/local`,
`make infra`, `make kafka`, then `make stream|silver|gold`. See root README §3.

## Definition of done (per phase)

1. All tasks in the phase's `tasks.md` are checked.
2. Every scenario in the capability specs the phase touches is satisfiable.
3. `make test` is green; new behavior has tests.
4. No decision in `decisions/decision-log.md` is violated.
