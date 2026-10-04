# Implementation roadmap (archived phase history)

Phases 0–15 are implemented and archived under `openspec/changes/archive/`.
This table records their original delivery sequence. It is not an active task
queue. Inspect `openspec/changes/` or run `openspec list --json` for current work.
For a new substantial change, use the [specification guide](README.md).

| # | Change | Delivers | Target capability specs |
|---|---|---|---|
| 0 | [`phase-0-foundation`](../openspec/changes/archive/2026-09-26-phase-0-foundation/proposal.md) | Repo skeleton, config, multi-tenancy + GDPR primitives, synthetic data generator, CI, tests | `foundation`, `synthetic-data` |
| 1 | [`phase-1-bronze`](../openspec/changes/archive/2026-09-26-phase-1-bronze/proposal.md) | Bronze layer: raw, immutable, tenant-partitioned Parquet (local + S3) | `bronze-ingestion` |
| 2 | [`phase-2-silver`](../openspec/changes/archive/2026-09-26-phase-2-silver/proposal.md) | Silver stage: JSON:API unwrap, canonical dim/fact tables, PII tokenization | `silver-curation` |
| 3 | [`phase-3-gold`](../openspec/changes/archive/2026-09-26-phase-3-gold/proposal.md) | Gold stage: per-subscriber credit & fraud feature tables | `gold-features` |
| 4 | [`phase-4-credit-ml`](../openspec/changes/archive/2026-09-26-phase-4-credit-ml/proposal.md) | Credit-limit model (PD + affordability sizing) + training | `ml-models` |
| 5 | [`phase-5-fraud-ml`](../openspec/changes/archive/2026-09-26-phase-5-fraud-ml/proposal.md) | Fraud model (rules + gradient boosting + anomaly) + scoring | `ml-models` |
| 6 | [`phase-6-serving`](../openspec/changes/archive/2026-09-26-phase-6-serving/proposal.md) | FastAPI scoring service + Gold→TimescaleDB publish; tenant-scoped | `serving-api` |
| 7 | [`phase-7-streaming`](../openspec/changes/archive/2026-09-26-phase-7-streaming/proposal.md) | Kafka producer/consumer bridge; Bronze from topics | `streaming-kafka` |
| 8 | [`phase-8-platform`](../openspec/changes/archive/2026-09-26-phase-8-platform/proposal.md) | K8s deployment (minikube/EKS), Helm services, Kafka cluster, namespaces, on-demand + orchestrated stages | `platform-deployment`, `orchestration` |
| 9 | [`phase-9-deploy-restructure`](../openspec/changes/archive/2026-09-26-phase-9-deploy-restructure/proposal.md) | Reorganize `deploy/` into `platform/` (infra) and `app/` (workloads) for team ownership; pure move-and-rewire | `platform-deployment`, `orchestration` |
| 10 | [`phase-10-app-overlays`](../openspec/changes/archive/2026-09-26-phase-10-app-overlays/proposal.md) | Move the scoring-api out of the platform overlays into its own `app/overlays/{local,aws}`; add `make app-deploy` | `platform-deployment` |
| 11 | [`phase-11-mcp-analytics`](../openspec/changes/archive/2026-09-26-phase-11-mcp-analytics/proposal.md) | Five read-only, tenant-scoped MCP analytics tools over Silver (subscribers, usage, recharges, recharge anomalies, fee charges) + synthetic fee-charge data | `mcp-analytics`, `synthetic-data`, `silver-curation` |
| 12 | [`phase-12-ml-store-explain`](../openspec/changes/archive/2026-09-26-phase-12-ml-store-explain/proposal.md) | Shared MinIO model store (`s3://idp-ml`) + `make train`; fee-charge features fed to the credit model; SHAP/scorecard adverse-action reason codes | `ml-models` |
| 13 | [`phase-13-training-inference-split`](../openspec/changes/archive/2026-09-26-phase-13-training-inference-split/proposal.md) | Train on historical Gold + a labels store (`s3://idp-ml/labels`, no synthetic regen); Gold features-only + separate `idp score` stage; two DAGs (silver→gold→score→publish, labels→train) | `ml-models`, `orchestration` |
| 14 | [`phase-14-dataset-driven-dags`](../openspec/changes/archive/2026-09-26-phase-14-dataset-driven-dags/proposal.md) | Three dataset-driven Airflow DAGs — `idp_features` (silver→gold), `idp_inference` (score→publish, scheduled on gold-features OR model), `idp_training` (labels→train); a new model auto-re-scores | `orchestration` |
| 15 | [`phase-15-score-distribution-tools`](../openspec/changes/archive/2026-09-26-phase-15-score-distribution-tools/proposal.md) | Two MCP score-distribution tools — `analyze_fraud_score_tool` and `analyze_credit_score_tool` — over the Gold score tables (7/15/30-day windows, tenant-scoped); `scored_at` stamp on `idp score` | `mcp-analytics` |

**Ordering rationale:** phases 0–6 originally built the runnable, testable core
without a cluster. Silver/Gold now run locally on Spark (JDK required).
Phase 7 attaches Kafka ingestion. Phase 8 deploys and
orchestrates. Real-time (Flink) scoring is intentionally out of scope (deferred;
see ADR-0004).

**Target capabilities** are defined in [`openspec/specs/`](../openspec/specs/). A phase is complete
when the scenarios of its listed capabilities are satisfied.
