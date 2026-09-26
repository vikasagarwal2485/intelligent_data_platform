# Tasks — Phase 8: Platform Deployment & Orchestration

## 1. Kustomize base & overlays
- [x] 1.1 `deploy/platform/base`: namespaces (`idp-platform`, `idp-app`, `idp-tenant-a/-b`),
      RBAC roles, per-tenant ResourceQuota + default-deny NetworkPolicy
- [x] 1.2 `deploy/app/deployment/scoring-api.yaml` in `idp-app`
- [x] 1.3 `platform/overlays/local` (`imagePullPolicy: Never`, dev image),
      `platform/overlays/aws` (ECR image, IRSA), `deploy/platform/argocd/application.yaml`

## 2. Platform services (Helm, non-Bitnami)
- [x] 2.1 `deploy/platform/install-infra.sh`: add repos + install MinIO (official chart),
      Strimzi, Nessie, Trino, TimescaleDB (manifest), Keycloak (manifest), Airflow,
      MLflow, kube-prometheus-stack; create buckets `idp-bronze/silver/gold`
- [x] 2.2 Tuned values + self-healing: `platform/trino/trino-values.yaml`+`platform/trino-tune.sh`,
      `platform/nessie/nessie-values.yaml`+`platform/nessie-tune.sh`, `platform/strimzi-tune.sh`

## 3. Kafka
- [x] 3.1 `deploy/platform/kafka/kafka-cluster.yaml` (KRaft NodePool + Kafka); node-pool
      `resources`; version placeholder
- [x] 3.2 `setup-kafka.sh`: auto-detect served API version + supported Kafka
      version; `schema-registry.yaml` (Apicurio); `create-topics.sh`;
      `register-schemas.sh`; `kafka-ui.yaml`

## 4. Jobs & orchestration
- [x] 4.1 `deploy/app/jobs/{produce,consume,silver,gold,publish}-job.yaml`
      (idp-app, cross-ns DNS, S3 env, `IDP_S3_BUCKET_PREFIX`; publish → TimescaleDB)
- [x] 4.2 Airflow: `deploy/platform/airflow/{values.yaml,rbac.yaml,Dockerfile}` (DAGs
      baked into a local image + RBAC to launch stage pods) and
      `pipelines/airflow/dags/idp_medallion_dag.py` (`silver >> gold` hourly + daily training DAG)
- [x] 4.3 `pipelines/spark/gold_job.py` + `deploy/app/jobs/spark-gold-job.yaml`
      (production Gold via spark-submit)
- [x] 4.4 `pipeline.run` composes stages locally (dev/test + training)
- [x] 4.5 Per-stage execution-engine switch in the DAG (`idp_silver_engine` /
      `idp_gold_engine` = pandas|spark): Airflow orchestrates; each task launches the
      pandas `idp` pod or a `spark-submit` pod, same DAG shape. `pipelines/spark/silver_job.py`
      (token-parity with pandas) + `spark-silver-job.yaml`; `publish` stays a separate
      task. Targets `make spark-configmap`, `spark-silver`, `silver-engine`, `gold-engine`.

## 5. Make targets & docs
- [x] 5.1 `mk-up`, `image`, `deploy`, `infra`, `kafka`, `stream`, `silver`, `gold`,
      `*-fix`, `minio-ls/reset/console`, `kafka-ui`
- [x] 5.2 Update root README §3 (batch vs streaming) and `deploy/README.md`

## 6. Team-oriented deploy layout (see phase-9-deploy-restructure)
- [x] 6.1 `deploy/platform/**` (infra) vs `deploy/app/**` (workloads) separation of concerns

## 7. Observability (amendment 2026-07-14) — REQ-PLT-05…08
- [x] 7.1 `deploy/platform/monitoring/kube-prometheus-values.yaml`: node-exporter +
      kube-state-metrics, Grafana + dashboard sidecar, Prometheus selects all
      PodMonitors — `REQ-PLT-05`, `REQ-PLT-06`, `REQ-PLT-07`
- [x] 7.2 Grafana dashboards (sidecar ConfigMaps): `dashboards/k8s-cluster-dashboard-configmap.yaml`
      (`REQ-PLT-07`), `dashboards/kafka-dashboard-configmap.yaml` (`REQ-PLT-08`)
- [x] 7.3 Kafka metrics: `metricsConfig` (JMX) + `kafkaExporter` on the Kafka CR,
      `kafka-metrics-configmap.yaml`, `kafka-podmonitor.yaml` — `REQ-PLT-08`
- [x] 7.4 Wiring: `install-infra.sh` applies values + dashboards; `setup-kafka.sh`
      applies the metrics ConfigMap before the CR; `make monitoring` + `make grafana-ui`
- [x] 7.5 Docs: `deploy/platform/monitoring/README.md`; ADR-0019

## Done when
`specs/platform-deployment` and `specs/orchestration` scenarios pass and the
minikube bring-up (`infra` → `kafka` → `stream` → `silver` → `gold`) works.
