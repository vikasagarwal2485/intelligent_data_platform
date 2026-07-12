# Phase 8 — Platform Deployment & Orchestration

## Why
Run the whole system on Kubernetes with clean namespace isolation, deploy the
platform services, and orchestrate the (separated) medallion stages on demand and
on a schedule.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- Kustomize `base` (namespaces `idp-platform`/`idp-app`/`idp-tenant-*`, RBAC,
  ResourceQuota, default-deny NetworkPolicy, scoring-api) + `overlays/local` and
  `overlays/aws` + Argo CD app — `REQ-PLT-01`, `REQ-PLT-04`.
- `deploy/install-infra.sh` (Helm repos + non-Bitnami services; per-layer buckets;
  tuned Trino/Nessie; self-healing reinstalls) — `REQ-PLT-02`.
- Kafka cluster (Strimzi KRaft) with API/version auto-detection; topics; Avro
  registration; Kafka UI — `REQ-PLT-03`.
- On-demand stage Jobs (`deploy/jobs/{silver,gold,publish}-job.yaml`,
  produce/consume jobs) — `REQ-ORC-01`.
- Airflow DAG (`silver >> gold` + daily training) and PySpark `gold_job.py`
  (+ `spark-gold-job.yaml`) — `REQ-ORC-02`.
- Local `pipeline.run` composition retained — `REQ-ORC-03`.
- Makefile targets: `mk-up`, `image`, `infra`, `kafka`, `stream`, `silver`, `gold`,
  `publish`, `*-fix`, `minio-*`, `airflow-*`, `spark-gold` — support `REQ-ORC-01/02`.

## Impact
- Spec deltas: `platform-deployment` (ADDED all), `orchestration` (ADDED all).
- Code: `deploy/**`, `pipelines/airflow/dags/idp_medallion_dag.py`,
  `pipelines/spark/gold_job.py`, `Makefile`.
- Decisions: ADR-0002, ADR-0003, ADR-0007, ADR-0009, ADR-0011, ADR-0012, ADR-0016, ADR-0017.

## Spec Deltas
Satisfies every requirement in `specs/platform-deployment` and `specs/orchestration`.
