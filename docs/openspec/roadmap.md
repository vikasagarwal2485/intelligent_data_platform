# Implementation Roadmap (phase order)

Implement phases **in ascending order**. Each phase is a self-contained OpenSpec
change under `changes/`. Do not start a phase until the previous one's
`tasks.md` is fully checked and `make test` is green.

| # | Change | Delivers | Target capability specs |
|---|---|---|---|
| 0 | [`phase-0-foundation`](changes/phase-0-foundation/proposal.md) | Repo skeleton, config, multi-tenancy + GDPR primitives, synthetic data generator, CI, tests | `foundation`, `synthetic-data` |
| 1 | [`phase-1-bronze`](changes/phase-1-bronze/proposal.md) | Bronze layer: raw, immutable, tenant-partitioned Parquet (local + S3) | `bronze-ingestion` |
| 2 | [`phase-2-silver`](changes/phase-2-silver/proposal.md) | Silver stage: JSON:API unwrap, canonical dim/fact tables, PII tokenization | `silver-curation` |
| 3 | [`phase-3-gold`](changes/phase-3-gold/proposal.md) | Gold stage: per-subscriber credit & fraud feature tables | `gold-features` |
| 4 | [`phase-4-credit-ml`](changes/phase-4-credit-ml/proposal.md) | Credit-limit model (PD + affordability sizing) + training | `ml-models` |
| 5 | [`phase-5-fraud-ml`](changes/phase-5-fraud-ml/proposal.md) | Fraud model (rules + gradient boosting + anomaly) + scoring | `ml-models` |
| 6 | [`phase-6-serving`](changes/phase-6-serving/proposal.md) | FastAPI scoring service + Gold→TimescaleDB publish; tenant-scoped | `serving-api` |
| 7 | [`phase-7-streaming`](changes/phase-7-streaming/proposal.md) | Kafka producer/consumer bridge; Bronze from topics | `streaming-kafka` |
| 8 | [`phase-8-platform`](changes/phase-8-platform/proposal.md) | K8s deployment (minikube/EKS), Helm services, Kafka cluster, namespaces, on-demand + orchestrated stages | `platform-deployment`, `orchestration` |
| 9 | [`phase-9-deploy-restructure`](changes/phase-9-deploy-restructure/proposal.md) | Reorganize `deploy/` into `platform/` (infra) and `app/` (workloads) for team ownership; pure move-and-rewire | `platform-deployment`, `orchestration` |
| 10 | [`phase-10-app-overlays`](changes/phase-10-app-overlays/proposal.md) | Move the scoring-api out of the platform overlays into its own `app/overlays/{local,aws}`; add `make app-deploy` | `platform-deployment` |
| 11 | [`phase-11-mcp-analytics`](changes/phase-11-mcp-analytics/proposal.md) | Five read-only, tenant-scoped MCP analytics tools over Silver (subscribers, usage, recharges, recharge anomalies, fee charges) + synthetic fee-charge data | `mcp-analytics`, `synthetic-data`, `silver-curation` |
| 12 | [`phase-12-ml-store-explain`](changes/phase-12-ml-store-explain/proposal.md) | Shared MinIO model store (`s3://idp-ml`) + `make train`; fee-charge features fed to the credit model; SHAP/scorecard adverse-action reason codes | `ml-models` |

**Ordering rationale:** phases 0–6 build the runnable, testable core in pure
Python (no cluster). Phase 7 attaches Kafka ingestion. Phase 8 deploys and
orchestrates. Real-time (Flink) scoring is intentionally out of scope (deferred;
see ADR-0004).

**Target capabilities** are defined in [`specs/`](specs/). A phase is complete
when the scenarios of its listed capabilities are satisfied.
