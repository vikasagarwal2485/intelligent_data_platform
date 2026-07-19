# Architecture Decision Log (ADR)

Binding architectural decisions for the Intelligent Data Platform. An agent MUST
respect these when implementing any phase. Format: Context → Decision →
Consequences. Status: `Accepted` unless noted.

---

### ADR-0001 — Medallion architecture (Bronze → Silver → Gold)
- **Status:** Accepted
- **Context:** Raw telecom events need progressive refinement into
  analytics/ML-ready data with clear data-quality gates.
- **Decision:** Adopt Bronze (raw, immutable) → Silver (clean, canonical,
  tenant-isolated, PII-tokenized) → Gold (per-subscriber features + scores).
- **Consequences:** Each layer is independently materialized and queryable; each
  transform is a separate stage (see ADR-0009).

### ADR-0002 — Iceberg for Silver, TimescaleDB for Gold serving
- **Status:** Accepted (Iceberg is the production target; prototype writes Parquet)
- **Context:** Silver is analytical & huge; Gold serving needs low-latency lookups.
- **Decision:** Silver = Apache Iceberg on object storage (ACID, schema/partition
  evolution, time-travel). Gold serving = TimescaleDB. Prototype uses plain
  Parquet on MinIO/S3 as an Iceberg stand-in.
- **Consequences:** Nessie catalog required (ADR-0011); production Silver/Gold
  jobs write Iceberg; prototype code writes Parquet with the same partitioning.

### ADR-0003 — Apache Kafka (open source) via Strimzi
- **Status:** Accepted
- **Context:** Need an event backbone; reviewer required open-source Kafka.
- **Decision:** Apache Kafka via the Strimzi operator; Avro contracts in a
  Confluent-compatible **Apicurio** Schema Registry. No Confluent/Redpanda.
- **Consequences:** KafkaNodePool/Kafka CRs (KRaft); topic names
  `t.<tenant>.<entity>` (no `tmf` segment, see ADR-0013).

### ADR-0004 — Batch-first; defer real-time (Flink)
- **Status:** Accepted
- **Context:** Reviewer chose to start with batch.
- **Decision:** Implement batch/micro-batch stages first; Flink real-time scoring
  is out of scope for the initial phases. Rule definitions are kept portable.
- **Consequences:** No Flink jobs in phases 0–8; streaming is micro-batch drain.

### ADR-0005 — Multi-tenancy: pooled platform, isolated data + RBAC
- **Status:** Accepted
- **Context:** Multiple tenants share the platform but must not see each other's data.
- **Decision:** Stamp `tenant_id` on every record at ingestion; carry it as the
  leading partition column and mandatory read predicate. Per-tenant Kafka
  ACLs/topics, S3 prefixes, K8s namespaces (quota + NetworkPolicy), and
  Keycloak(OIDC)+OPA RBAC.
- **Consequences:** All lake access goes through a tenant-scoping gateway; a
  cross-tenant read is a defect.

### ADR-0006 — GDPR PII tokenization (per-tenant salted hash)
- **Status:** Accepted
- **Context:** PII (MSISDN/IMSI/IMEI/phone/email) must not persist raw in the lake.
- **Decision:** Tokenize with `tok_<sha256(per_tenant_salt + value)>` at Silver.
  Deterministic per tenant so tokens still join; consent (`privacy-consent`)
  gates ML use; support right-to-erasure and retention.
- **Consequences:** Silver never stores raw identifiers; joins use tokens.

### ADR-0007 — Namespace split: idp-platform vs idp-app
- **Status:** Accepted
- **Context:** Shared services and application workloads have different lifecycles.
- **Decision:** Platform services (Kafka, MinIO, Trino, Nessie, TimescaleDB,
  Keycloak, Airflow, MLflow, Prometheus) in `idp-platform`; application workloads
  (scoring API, medallion Jobs) in `idp-app`; per-tenant isolation namespaces
  `idp-tenant-*`. Apps reach services via cross-namespace DNS.
- **Consequences:** Jobs use `<svc>.idp-platform.svc` addresses.

### ADR-0008 — One MinIO/S3 bucket per medallion layer
- **Status:** Accepted (supersedes an earlier single-bucket+prefix approach)
- **Context:** A single `idp-bronze` bucket holding silver/gold prefixes was confusing.
- **Decision:** `s3://idp-bronze`, `s3://idp-silver`, `s3://idp-gold` (prefix
  `idp` configurable). `lake_uri(layer)` → `<prefix>-<layer>`.
- **Consequences:** `install-infra.sh` provisions all three buckets.

### ADR-0009 — Separation of concerns: independent medallion stages
- **Status:** Accepted (supersedes consumer→silver→gold chaining)
- **Context:** The consumer previously chained Bronze→Silver→Gold in one job.
- **Decision:** Each stage is an independent function/job reading its input layer
  from storage and writing its output layer: `to_bronze` (Kafka→Bronze),
  `to_silver` (Bronze→Silver), `to_gold` (Silver→Gold), `train` (labelled→models).
  `run()` only composes them locally.
- **Consequences:** CLI `idp consume|silver|gold|train`; separate K8s Jobs;
  Airflow orchestrates Silver→Gold; Spark implements production Gold.

### ADR-0010 — Prototype in pandas/scikit-learn; document the production engines
- **Status:** Accepted
- **Context:** The platform must be unit-testable without a cluster while staying
  faithful to the production design.
- **Decision:** Implement stage/ML logic in pandas + scikit-learn. Use
  `HistGradientBoostingClassifier` as a drop-in for LightGBM/XGBoost. Spark/Flink/
  Iceberg are the production engines (deployed + documented, e.g. `gold_job.py`).
- **Consequences:** Tests run in CI without infra; production ports reuse the same logic.

### ADR-0011 — Nessie as the Iceberg catalog
- **Status:** Accepted
- **Context:** Iceberg needs a catalog; options include Hive, Glue, Polaris, Nessie.
- **Decision:** Nessie (git-like, branchable) — reviewer decision.
- **Consequences:** Deploy Nessie (in-memory store for prototype); production uses
  a persistent store.

### ADR-0012 — Airflow for data orchestration
- **Status:** Accepted
- **Context:** Need to schedule/coordinate the medallion + training pipelines.
- **Decision:** Apache Airflow (data-first DAGs, backfill, SLAs). Argo reserved
  for CI/CD if needed.
- **Consequences:** `idp_medallion_dag.py` orchestrates Silver→Gold + daily training.

### ADR-0013 — Avro contracts; drop the `tmf` segment from topics/subjects
- **Status:** Accepted
- **Context:** Topic/subject naming and schema management.
- **Decision:** Avro schemas in `config/avro/`; register via Apicurio
  (`<topic>-value` subjects). Topics are `t.<tenant>.<entity>` (no `tmf`); Avro
  record namespace `com.idp`.
- **Consequences:** Producers/consumers use these names; registry auto-detected.

### ADR-0014 — Synthetic data generator; labels stay out of the lake
- **Status:** Accepted
- **Context:** `data_samples/` are single illustrative docs; ML needs volume + labels.
- **Decision:** A deterministic, multi-tenant, labelled synthetic generator is a
  Phase-0 deliverable. Ground-truth labels (fraud/default) exist only in-process
  for training; they do not flow through Kafka/Bronze/Silver.
- **Consequences:** Training (`idp train`) is a separate concern from the ELT
  stages; streaming Gold produces features and *applies* trained models only.

### ADR-0015 — ML designs: two-stage credit, layered fraud
- **Status:** Accepted
- **Context:** Alt-credit has no bureau label; fraud is rare/adversarial.
- **Decision:** Credit = PD classifier (+ interpretable scorecard baseline) then
  affordability × (1−PD) limit sizing. Fraud = rules + gradient-boosted classifier
  + IsolationForest, fused into one score. Evaluate out-of-sample (AUC/KS; PR-AUC/
  precision@k).
- **Consequences:** Explainability (SHAP) and calibration are part of the credit spec.

### ADR-0016 — minikube (local) / EKS (prod); Kustomize + Argo CD
- **Status:** Accepted
- **Context:** Reviewer chose K8s on both tiers.
- **Decision:** Same Kustomize base with `overlays/local` (minikube, `imagePullPolicy:
  Never`) and `overlays/aws` (EKS, ECR, IRSA). GitOps via Argo CD.
- **Consequences:** No docker-compose; image built into minikube for local.

### ADR-0017 — Avoid Bitnami charts for the prototype
- **Status:** Accepted
- **Context:** Bitnami removed free image tags (2025-08), breaking installs.
- **Decision:** Use non-Bitnami sources with freely pullable images (official
  MinIO chart, `timescale/timescaledb`, `quay.io/keycloak`, Strimzi, Trino,
  Nessie, Airflow, MLflow, kube-prometheus-stack).
- **Consequences:** `install-infra.sh` pins these; tuned values for Trino/Nessie.

### ADR-0018 — Deploy layout split by owning team: `platform/` vs `app/`
- **Status:** Accepted
- **Context:** A flat `deploy/` mixed shared infrastructure with application
  workloads; different engineering teams own those, so the flat layout blurred
  ownership, review boundaries, and access control.
- **Decision:** Reorganize into `deploy/platform/**` (platform team: base/overlays,
  argocd, kafka, keycloak, timescaledb, trino, nessie, airflow, install/tune
  scripts) and `deploy/app/**` (application team: `deployment/scoring-api`,
  `jobs/*`). scoring-api moves out of the Kustomize base into `app/deployment`;
  the overlays compose `../../base` + `../../../app/deployment`.
- **Consequences:** Pure move-and-rewire (`git mv` + path updates); no chart,
  image, or runtime behavior change, so no cluster re-install is required.

### ADR-0019 — Observability via kube-prometheus-stack + Strimzi metrics
- **Status:** Accepted (2026-07-14)
- **Context:** REQ-PLT-05…08 require node/cluster metrics, Grafana, a Kubernetes
  cluster dashboard, and a Kafka produce/consume dashboard.
- **Decision:** Use the kube-prometheus-stack (Prometheus Operator, Prometheus,
  Grafana, node-exporter, kube-state-metrics) for cluster observability, with the
  Grafana dashboard sidecar loading dashboards from labelled ConfigMaps. Expose
  Kafka via the Strimzi JMX Prometheus exporter (`metricsConfig`) + Kafka Exporter
  (`kafkaExporter`), scraped by a `PodMonitor`. Ship two dashboards (cluster
  overview, Kafka produce/consume) as sidecar ConfigMaps.
- **Consequences:** No bespoke Prometheus/Grafana wiring; dashboards are
  version-controlled ConfigMaps. Kafka metrics require re-applying the Kafka CR
  (`make kafka`) so the broker exposes the JMX port.

### ADR-0020 — Deploy the app separately from the platform base (own overlays)
- **Status:** Accepted (2026-07-15)
- **Context:** After the platform/app split (ADR-0018) the scoring-api app and its
  env-specific patch still lived inside the *platform* overlays, so `make
  platform-base` deployed a platform concern (namespaces/RBAC) and an app concern
  (scoring-api) together — muddying ownership.
- **Decision:** Platform overlays (`platform/overlays/{local,aws}`) carry only the
  base (namespaces/RBAC/isolation). The scoring-api gets its own overlays
  (`app/overlays/{local,aws}`) that reference the app base and hold the
  env-specific image/replicas/pull-policy patch. New `make app-deploy` applies the
  app; two Argo CD Applications (sync-waves) apply platform base then app.
- **Consequences:** Bring-up gains a step (`… platform-base → platform-services →
  app-deploy → …`). Each team applies and rolls back its own kustomization. The
  patch lives in app *overlays* (not the app base) so local vs AWS stay distinct.

### ADR-0021 — MCP analytics tools read Silver via pandas, stdio server
- **Status:** Accepted (2026-07-19)
- **Context:** REQ-MCP-01…06 expose the lake to AI agents as five read-only,
  tenant-scoped analytics tools. Two questions: what the tools read from, and how
  they are served.
- **Decision:** Each tool is a pure function that reads the tenant's **Silver**
  tables (`dim_subscriber`/`fact_usage`/`fact_recharge`/`fact_fee_charge`) with the
  existing `common.storage` pandas helpers and returns a JSON-able aggregate — no
  raw PII, no cross-tenant reads. They are registered on an **MCP stdio server**
  (`idp mcp`), the conventional surface for `mcp` clients / Claude Desktop. An
  in-cluster streamable-HTTP Deployment is a later follow-up.
- **Consequences:** Tools are unit-testable without a cluster (same pandas path as
  the rest of the prototype); Trino is not a runtime dependency of the analytics
  surface. Very large tenants would eventually want push-down SQL (deferred).

### ADR-0022 — Synthetic fee-charge data is additive to the generator
- **Status:** Accepted (2026-07-19)
- **Context:** The fee-charge tool (REQ-MCP-05) has no source data — `products`
  carried no price and there was no fee-charge event stream. `recharge_type` was
  also a constant, making its split degenerate.
- **Decision:** Extend the deterministic generator **additively**: a new
  `fee_charges` fact (recurring/one-time/overage/discount), a `category`+
  `monthly_fee` price book on `products`, and a weighted `recharge_type` draw. No
  change to labels, features, or the ML models — fee charges are analytics-only in
  this phase.
- **Consequences:** Existing tables/columns and tests are unaffected (additive
  only). Fee charges are available to Silver/MCP but intentionally **not** fed to
  the credit/fraud models yet (a possible later feature-engineering change).
