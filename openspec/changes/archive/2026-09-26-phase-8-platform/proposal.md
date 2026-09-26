# Phase 8 — Platform Deployment & Orchestration

## Why
Run the whole system on Kubernetes with clean namespace isolation, deploy the
platform services, and orchestrate the (separated) medallion stages on demand and
on a schedule.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- Kustomize `platform/base` (namespaces `idp-platform`/`idp-app`/`idp-tenant-*`,
  RBAC, ResourceQuota, default-deny NetworkPolicy) + `app/deployment/scoring-api`
  + `platform/overlays/local` and `platform/overlays/aws` + Argo CD app —
  `REQ-PLT-01`, `REQ-PLT-04`.
- `deploy/platform/install-infra.sh` (Helm repos + non-Bitnami services; per-layer
  buckets; tuned Trino/Nessie; self-healing reinstalls) — `REQ-PLT-02`.
- Kafka cluster (Strimzi KRaft) with API/version auto-detection; topics; Avro
  registration; Kafka UI — `REQ-PLT-03`.
- On-demand stage Jobs (`deploy/app/jobs/{produce,consume,silver,gold,publish}-job.yaml`)
  — `REQ-ORC-01`.
- Airflow DAG (`silver >> gold` + daily training) and PySpark `gold_job.py`
  (+ `spark-gold-job.yaml`) — `REQ-ORC-02`.
- Local `pipeline.run` composition retained — `REQ-ORC-03`.
- Makefile targets: `mk-up`, `image`, `infra`, `kafka`, `stream`, `silver`, `gold`,
  `publish`, `*-fix`, `minio-*`, `airflow-*`, `spark-gold` — support `REQ-ORC-01/02`.
- Observability (added 2026-07-14): kube-prometheus-stack values
  (`deploy/platform/monitoring/kube-prometheus-values.yaml`) — node-exporter +
  kube-state-metrics + Grafana + dashboard sidecar — `REQ-PLT-05`, `REQ-PLT-06`,
  `REQ-PLT-07`.
- Grafana dashboards (`deploy/platform/monitoring/dashboards/k8s-cluster-dashboard-configmap.yaml`,
  `deploy/platform/monitoring/dashboards/kafka-dashboard-configmap.yaml`) —
  `REQ-PLT-07`, `REQ-PLT-08`.
- Kafka metrics (`deploy/platform/kafka/kafka-cluster.yaml` metricsConfig +
  kafkaExporter, `deploy/platform/monitoring/kafka-metrics-configmap.yaml`,
  `deploy/platform/monitoring/kafka-podmonitor.yaml`) — `REQ-PLT-08`.
- Make targets `monitoring` + `grafana-ui` — support `REQ-PLT-05…08`.

## Impact
- Spec deltas: `platform-deployment` (ADDED all), `orchestration` (ADDED all).
- Code: `deploy/platform/**`, `deploy/app/**`, `pipelines/airflow/dags/idp_medallion_dag.py`,
  `pipelines/spark/gold_job.py`, `Makefile`.
- Decisions: ADR-0002, ADR-0003, ADR-0007, ADR-0009, ADR-0011, ADR-0012, ADR-0016, ADR-0017.

## Amendment 2026-07-14 — Observability (REQ-PLT-05…08)
New requirements added to `specs/platform-deployment` (see its Change history).
Implementation, each item references the Requirement id(s) it satisfies:
- kube-prometheus-stack values (`deploy/platform/monitoring/kube-prometheus-values.yaml`):
  node-exporter + kube-state-metrics on; Grafana with Prometheus datasource + a
  dashboard sidecar; Prometheus selects all PodMonitors — `REQ-PLT-05`,
  `REQ-PLT-06`, `REQ-PLT-07`.
- Grafana dashboards as sidecar ConfigMaps: "IDP — Kubernetes Cluster Overview"
  (`dashboards/k8s-cluster-dashboard-configmap.yaml`) — `REQ-PLT-07`; "IDP —
  Kafka Produce/Consume" (`dashboards/kafka-dashboard-configmap.yaml`) — `REQ-PLT-08`.
- Kafka metrics: Strimzi `metricsConfig` (JMX exporter) + `kafkaExporter` on the
  Kafka CR, `kafka-metrics` ConfigMap, and `kafka-podmonitor.yaml` scrape —
  `REQ-PLT-08`.
- Make targets `monitoring` + `grafana-ui`; `install-infra.sh` applies the values
  and dashboards; `setup-kafka.sh` applies the metrics ConfigMap before the CR —
  support `REQ-PLT-05…08`.
- Decision: ADR-0019 (observability via kube-prometheus-stack + Strimzi metrics).

## Spec Deltas
Satisfies every requirement in `specs/platform-deployment` (incl. the 2026-07-14
REQ-PLT-05…08 observability additions) and `specs/orchestration`.
