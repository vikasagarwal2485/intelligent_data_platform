# Phase 8 — Platform Deployment & Orchestration

## Why
Run the whole system on Kubernetes with clean namespace isolation, deploy the
platform services, and orchestrate the (separated) medallion stages on demand and
on a schedule.

## What Changes
- Kustomize `base` (namespaces `idp-platform`/`idp-app`/`idp-tenant-*`, RBAC,
  ResourceQuota, default-deny NetworkPolicy, scoring-api) + `overlays/local`
  (minikube, `imagePullPolicy: Never`) and `overlays/aws` (EKS/ECR/IRSA) + Argo CD app.
- `deploy/install-infra.sh` (Helm repos + non-Bitnami services; per-layer buckets)
  with tuned values (Trino/Nessie) and self-healing reinstalls.
- Kafka cluster (Strimzi KRaft) with API/version auto-detection; topics; Avro
  registration; Kafka UI.
- On-demand stage Jobs (`deploy/jobs/{silver,gold}-job.yaml`, produce/consume jobs);
  Airflow DAG (`silver >> gold` + daily training); PySpark `gold_job.py`.
- Makefile targets: `mk-up`, `image`, `infra`, `kafka`, `stream`, `silver`, `gold`,
  `*-fix`, `minio-*`.

## Impact
- Spec deltas: `platform-deployment` (ADDED all), `orchestration` (ADDED all).
- Code: `deploy/**`, `pipelines/airflow/dags/idp_medallion_dag.py`,
  `pipelines/spark/gold_job.py`, `Makefile`.
- Decisions: ADR-0002, ADR-0003, ADR-0007, ADR-0009, ADR-0011, ADR-0012, ADR-0016, ADR-0017.

## Spec Deltas
Satisfies every requirement in `specs/platform-deployment` and `specs/orchestration`.
