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
- **Status:** Accepted — the Parquet stand-in is superseded by ADR-0028 (all layers are Iceberg)
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
- **Status:** Superseded by ADR-0027 (2026-09-26)
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
  a persistent store. *Note (ADR-0028):* the lake's tables are catalogued in an
  Iceberg JDBC catalog, not Nessie, because Nessie's REST catalog keeps only a
  table's current snapshot.

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
- **Status:** Superseded by ADR-0030 (2026-09-29); accepted 2026-07-19
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

### ADR-0023 — Fee-charge features feed the credit model
- **Status:** Accepted (2026-07-20) — refines ADR-0022 for the credit model
- **Context:** ADR-0022 kept fee charges analytics-only. Product-fee commitment
  (recurring fees, overages) is a genuine spend/affordability signal for credit.
- **Decision:** Enrich Silver `fact_fee_charge` with `agreement_id` (from the
  product join) so fees attribute to a subscriber (`agreement_id → service_id`),
  and add `fee_total`/`fee_recurring_total`/`fee_overage_count`/`fee_charge_count`/
  `fee_total_30d` to `CREDIT_FEATURES`. The fraud model is left unchanged (fee
  signals are weak fraud indicators; avoids destabilizing its features).
- **Consequences:** The credit model retrains on a wider feature set. Fees remain
  normalized at the raw layer (product_id only); the agreement link and the
  per-subscriber attribution are Silver/feature-time joins.

### ADR-0024 — Shared MinIO model store + adverse-action reason codes
- **Status:** Accepted (2026-07-20)
- **Context:** `train`, `gold` and `scoring-api` are separate pods; models were
  saved to an ephemeral per-pod `ml/artifacts` dir, so Gold/serving never saw the
  trained models and Gold silently emitted feature tables with no scores. Also no
  per-decision explainability existed.
- **Decision:** Persist models to an object-store **model store** — `s3://<prefix>-ml`
  (MinIO/S3) when S3 is enabled, else a local dir — via `storage.save_model/
  load_model/model_exists` (joblib over fsspec, chosen by URI scheme). `train`
  writes; `to_gold` and the scoring API read; a new `train-job.yaml`/`make train`
  runs training in-cluster; the scoring API Deployment gets the S3 env. Add
  `CreditLimitModel.reason_codes()` — SHAP `TreeExplainer` when the optional `shap`
  extra is installed, else logistic-scorecard contributions — surfaced in the
  `/credit-limit` response.
- **Consequences:** Bring-up gains a `make train` step before `make gold`. Model
  artifacts are shared and durable (survive pod restarts). SHAP stays optional so
  the image isn't bloated; the scorecard fallback is the regulatory-standard
  adverse-action method and always available.

### ADR-0025 — Train on historical Gold + a labels store; split Gold/Score
- **Status:** Accepted (2026-07-20) — refines ADR-0014
- **Context:** ADR-0014 kept labels out of the lake, so `train` regenerated
  synthetic data in memory instead of learning from historical lake data. Scoring
  was also folded into `to_gold`, coupling feature-build with inference and forcing
  a "train before gold" order.
- **Decision:** Persist ground-truth labels to a dedicated **labels/outcomes store**
  (`s3://<prefix>-ml/labels`), tokenized and kept OUTSIDE the Gold/serving path so
  they never leak into features or the published serving tables. `train` reads
  historical **Gold features + the labels store** (no synthetic regeneration; a
  local in-memory path remains for dev/tests). Make **Gold features-only** and add
  a separate **Score** stage (`idp score`) for inference. Orchestrate as two DAGs:
  refresh `silver >> gold >> score >> publish`, and daily `labels >> train`.
- **Consequences:** Refines ADR-0014 — labels now have a home, but a governed one
  (ML area only, never serving). Training learns from the lake and runs on its own
  cadence; feature-build and inference are decoupled. Bring-up gains `make labels`
  and `make score`. In production the `labels` task is replaced by the real
  observed-outcomes feed.

### ADR-0026 — Three dataset-driven DAGs (features / inference / training)
- **Status:** Accepted (2026-07-20) — refines ADR-0025
- **Context:** Phase 13 folded `score` into the refresh DAG (`silver >> gold >>
  score >> publish`). Scoring depends on a model produced by the training DAG on a
  different cadence, so cold start wasted a score run and a newly trained model
  only reached serving on a manual re-trigger — the three concerns (feature build,
  inference, training) were coupled to one cadence.
- **Decision:** Split into three DAGs wired by Airflow **Datasets**:
  `idp_features` (`silver >> gold`, produces the gold-features dataset),
  `idp_training` (`labels >> train`, produces the model dataset), and
  `idp_inference` (`score >> publish`) scheduled on `(GOLD_FEATURES | MODEL)` — OR
  semantics (Airflow >= 2.9) so it re-runs on a feature refresh OR a new model.
  Inference always loads the current model from the store, so promotion
  auto-re-scores with no manual pass. Each stage stays individually runnable
  (`make` targets unchanged).
- **Consequences:** Three DAGs instead of two and a dependency on Airflow >= 2.9
  dataset conditions, but the cold-start double pass is gone, new models propagate
  to serving automatically, and the three cadences are independent. `publish` stays
  in the inference DAG (re-pushes features + scores each run — a harmless replace).

### ADR-0027 — Spark for data transforms; pandas and scikit-learn for ML only
- **Status:** Accepted (2026-09-26) — supersedes ADR-0010; the cluster runtime is refined by ADR-0029 (Spark Operator)
- **Context:** ADR-0010 put the Silver/Gold logic in pandas so the prototype was
  testable without a cluster, with PySpark jobs as the "production" port. That
  left every business rule implemented twice (`curation/silver.py` +
  `features/*` vs `pipelines/spark/*_job.py`), kept in step only by a parity test,
  and the pandas path loads whole tables into one process — infeasible at telecom
  scale (a 5 M-subscriber operator needs ~22 B usage events for a 90-day window).
  The Spark jobs were proven equal to pandas (max |diff| ~1e-13) and run locally in
  CI, so the testability argument no longer holds.
- **Decision:** One implementation of the Silver and Gold transforms, on Apache
  Spark (`src/idp/transforms/`), used by the CLI stages, the Kubernetes Jobs,
  Airflow and the tests. The execution environment only selects the Spark master
  and resources (`local[*]` locally/in CI/in a single pod; a cluster later —
  `IDP_SPARK_MASTER`, `IDP_SPARK_DRIVER_MEMORY`); there is no engine switch.
  Stages exchange data only through the lake. pandas and scikit-learn are confined
  to ML (training on the labelled Gold feature set, scoring) plus the synthetic
  generator; the Bronze landing, publish and the MCP reads stay on pandas until
  their roadmap changes (Iceberg/incremental ingestion, distributed runtime, MCP
  over Trino). The pandas outputs were frozen as golden files
  (`tests/golden/`) before the pandas code was removed.
- **Consequences:** No duplicated business logic; the transforms scale out once a
  cluster master is configured. A JDK is a prerequisite for dev and CI, the `idp`
  image carries a JRE + pyspark + the S3A jars (one image for every stage; the
  `apache/spark` pods and script ConfigMaps are gone), and the test suite starts a
  local Spark session. Small runs pay JVM start-up. REQ-ORC-04 (engine parity) is
  retired; the golden-file tests guard behaviour instead.

### ADR-0028 — Iceberg lake on a JDBC catalog; incremental Silver facts; Gold full recompute
- **Status:** Accepted (2026-09-27) — supersedes ADR-0002's Parquet stand-in. Amends
  ADR-0011: Nessie is not the lake's catalog.
- **Context:** Bronze, Silver and Gold were plain Parquet. Every Silver run re-read
  all of Bronze, so its cost grew with history. A rerun rewrote whole tenant
  partitions, and readers could see a half-written table.
- **Decision:**
  - Bronze, Silver and Gold are **Iceberg tables** (namespaces
    `bronze`/`silver`/`gold`), each in its layer's bucket under `tables/`.
  - The catalog is an **Iceberg JDBC catalog** that `pyiceberg` (SqlCatalog) and
    Spark (JdbcCatalog) share: the `iceberg` database on the platform
    TimescaleDB/Postgres in the cluster, and a SQLite file locally and in CI
    (`IDP_CATALOG_TYPE=sql`, `IDP_CATALOG_URI`).
  - `idp consume` appends to Bronze through `pyiceberg`, partitioned by
    `(tenant_id, ingest_date)`, and commits the Kafka offsets only after the append.
  - **Silver facts are incremental.** Structured Streaming reads only the Bronze
    snapshots appended since the last run (`Trigger.Once` plus a checkpoint per
    fact/source), and merges them insert-if-absent on `(tenant_id, id)`.
    `dim_subscriber` is rebuilt in full. `--full-refresh` replays all of Bronze.
  - **Gold is a full recompute** per tenant (`overwritePartitions`). Scores are
    appended.
  - `idp maintain` (the daily DAG) compacts files, expires snapshots and removes
    orphan files. Expiry never removes a Bronze snapshot that a checkpoint still
    references. Bronze is archived by an object-store lifecycle rule
    (`idp archive-policy`), never deleted.
  - `idp migrate-lake` moves a legacy Parquet Bronze across as one snapshot per
    entity.
- **Rejected: Nessie's Iceberg REST catalog.** It was the original plan, and was
  deployed and probed. It keeps only a table's **current** snapshot in its metadata
  (history lives in Nessie commits: three appends left one snapshot). That breaks
  the snapshot-referencing streaming checkpoints, leaves nothing for
  `expire_snapshots`, and makes `remove_orphan_files` unsafe (Nessie GC would be
  needed). It also ignores client table locations and, by default, signs S3
  requests only under its warehouse.
- **Consequences:**
  - Each run commits atomically, so readers see the previous snapshot or the new
    one.
  - Silver cost follows new data, not history.
  - The platform Postgres gains one database, and the image bakes the Iceberg
    runtime, the AWS bundle, and the SQLite and Postgres JDBC drivers.
  - `Trigger.AvailableNow` can't be used: Iceberg re-validates the stream's first
    snapshot on every restart, which conflicts with snapshot expiry.
  - Without a remote tier, MinIO rejects the transition rule, so on minikube the
    archive rule is rendered only.
  - Labels stay Parquet in `idp-ml` (ADR-0014).
  - Nessie remains provisioned for a future branch/tag workflow.

### ADR-0029 — Spark Operator runtime; distributed score and publish; bounded training
- **Status:** Accepted (2026-09-28) — refines ADR-0027's runtime
- **Context:** After ADR-0027, Silver and Gold ran on Spark, but only as `local[*]`
  inside one pod. So a stage could process no more than one container's memory, and
  nothing scaled out. Three stages still pulled whole tables into one Python
  process: Score (all Gold features into pandas), publish (pandas `to_sql`) and
  train (the full labelled set). An earlier "Spark OOM" turned out to be the
  minikube node container's memory cap (6 GB, invisible to Kubernetes), not a pod
  limit.
- **Decision:**
  - **Spark Operator.** Silver, Gold, Score, publish and maintenance run as Kubeflow
    Spark Operator `SparkApplication`s: a driver plus executors, all from the `idp`
    image.
    - Each stage's resources come from `spark.profiles.<stage>`, and every pod's
      limit is heap plus overhead.
    - `idp spark-manifest <stage>` renders the manifest. The `make` targets apply it
      and wait for the final state, and Airflow submits it with
      `SparkKubernetesOperator`, failing the task if the application fails.
    - The image gets a Spark-on-Kubernetes entrypoint for the `driver`/`executor`
      pods.
    - Local runs and CI keep `local[*]`.
  - **Distributed scoring.**
    - The models are broadcast, and each partition of the feature table is scored
      with `mapInPandas`.
    - `FraudModel` normalizes its anomaly score over the population's min/max
      (computed in one Spark pass), so the scores equal whole-table scoring exactly.
  - **Distributed publish.**
    - Spark loads each serving table in parallel over JDBC into a uniquely named
      staging table.
    - One driver transaction then swaps it in: advisory lock, drop the live table,
      build the unique `(tenant_id, service_id)` index, rename.
    - Duplicate keys fail the swap and leave the old table in place.
  - **Bounded training.** `train` joins features and labels in Spark and fits on at
    most `ml.train_max_rows` rows. The sample is exact and stratified per
    (tenant, label class) with a fixed seed, and the metrics record `sampled_rows`
    and `sample_seed`.
  - **Node headroom.** `make mk-check` verifies that the node's real memory cap
    holds the platform plus the largest stage profile.
- **Consequences:**
  - No stage holds a whole table in one process, and a stage scales by adding
    executors, with no code change.
  - The platform gains the operator (a controller and a webhook). Stage runs are
    SparkApplications, and the single-pod Jobs remain only for rollback.
  - The minikube defaults use 1 GiB executors so a stage fits the 12 GB node; EKS
    profiles use more and larger executors.
  - Serving tables now come from a Spark JDBC load, so `scored_at` there is a UTC
    `TIMESTAMP` column. The lake keeps the time zone.
  - Model loading goes through one seam, `transforms.scoring.load_scoring_models`,
    which a model registry replaces later.

### ADR-0030 — MCP analytics tools query the lake through Trino as a read-only identity
- **Status:** Accepted (2026-09-29) — supersedes ADR-0021
- **Context:** ADR-0021's tools read each tenant's tables into pandas in the MCP
  process. After ADR-0028 that meant `pyiceberg` scans with a tenant filter, but still
  the whole tenant slice in memory (694,859 usage rows at 10× volume), and read-only
  was only a property of the code, with lake credentials on the operator's host.
  Trino 480 was installed but had no catalogs.
- **Decision:**
  - **Trino `lake` catalog** on the lake's Iceberg JDBC catalog (the TimescaleDB
    `iceberg` database), with native S3 to MinIO. The catalog name is pinned to
    `lake` (`catalog.name`, `IDP_CATALOG_NAME`) instead of a hash of the connection
    URI, so Spark, `pyiceberg` and Trino address the same rows and a password
    rotation can't change it; the existing rows were renamed once.
  - **Read-only identity.** File-based access control gives `idp-mcp` `SELECT` on
    `lake.silver` and `lake.gold` only; `admin` may do anything, other users
    nothing. The engine refuses writes, DDL and Bronze reads.
  - **Tools as SQL.** Each tool runs a few queries through `mcp.sql` with DB-API `?`
    binding: the tenant, `sensitivity` and `limit` are always parameters, and table
    and column names are code constants (granularity through an allow-list).
    Filtering, grouping, sums, histograms and rank statistics run in Trino over the
    tenant's partitions; Python only shapes the grouped results. Every result keeps
    `tenant_id` and passes through `scope_to_tenant`.
  - **Exact rank statistics.** Medians, the anomaly MAD and percentiles are exact,
    from window-function order statistics, interpolated as numpy does. REQ-MCP-09
    allows ±1% rank error, but Trino's T-Digest `approx_percentile` exceeded it on
    small or discrete data.
  - **Error contract kept:** unknown tenant → `available_tenants` +
    `action_required`; unreachable engine or access denied → `error` + `source`
    (the Trino endpoint); a table not written yet → empty.
  - **Testing.** The pandas implementations are frozen as the oracle
    (`tests/mcp_reference.py`). Unit tests use a fake query engine; `pytest -m trino`
    builds a lake through the real stages in a compose stack (Trino + Postgres +
    MinIO, the cluster's catalog file and rules) and compares every tool with the
    oracle, the IO plan's tenant constraint, the rows fetched, and the refused writes.
- **Consequences:**
  - The MCP process holds only aggregates, and the host needs only Trino
    (`make trino-api`, 127.0.0.1:8085), no lake credentials. `make mcp-config-minio`
    is gone.
  - Trino is now a runtime dependency of the analytics surface. Its coordinator
    (768M heap, about 0.9 GB resident) competes with Spark stages on the 12 GB
    minikube node, and its JDBC catalog adds Postgres connections.
  - **Dev authentication is header-based** (`X-Trino-User`) inside the cluster
    network, so a client can claim any user; access control still limits even a
    spoofed `idp-mcp` to reads. Production needs TLS plus password or OIDC
    authentication, and per-end-user row-level security stays out of scope.
  - The official MinIO images are no longer publicly pullable; the test stack uses
    Bitnami's frozen legacy build of the cluster's MinIO commit.
