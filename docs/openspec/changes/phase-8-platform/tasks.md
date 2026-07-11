# Tasks — Phase 8: Platform Deployment & Orchestration

## 1. Kustomize base & overlays
- [ ] 1.1 `deploy/base`: namespaces (`idp-platform`, `idp-app`, `idp-tenant-a/-b`),
      RBAC roles, per-tenant ResourceQuota + default-deny NetworkPolicy
- [ ] 1.2 `deploy/base/scoring-api.yaml` in `idp-app`
- [ ] 1.3 `overlays/local` (`imagePullPolicy: Never`, dev image), `overlays/aws`
      (ECR image, IRSA), `deploy/argocd/application.yaml`

## 2. Platform services (Helm, non-Bitnami)
- [ ] 2.1 `deploy/install-infra.sh`: add repos + install MinIO (official chart),
      Strimzi, Nessie, Trino, TimescaleDB (manifest), Keycloak (manifest), Airflow,
      MLflow, kube-prometheus-stack; create buckets `idp-bronze/silver/gold`
- [ ] 2.2 Tuned values + self-healing: `trino-values.yaml`+`trino-tune.sh`,
      `nessie-values.yaml`+`nessie-tune.sh`, `strimzi-tune.sh`

## 3. Kafka
- [ ] 3.1 `deploy/kafka/kafka-cluster.yaml` (KRaft NodePool + Kafka); node-pool
      `resources`; version placeholder
- [ ] 3.2 `setup-kafka.sh`: auto-detect served API version + supported Kafka
      version; `schema-registry.yaml` (Apicurio); `create-topics.sh`;
      `register-schemas.sh`; `kafka-ui.yaml`

## 4. Jobs & orchestration
- [ ] 4.1 `deploy/kafka/{produce,consume}-job.yaml`, `deploy/jobs/{silver,gold,publish}-job.yaml`
      (idp-app, cross-ns DNS, S3 env, `IDP_S3_BUCKET_PREFIX`; publish → TimescaleDB)
- [ ] 4.2 Airflow: `deploy/airflow/{values.yaml,rbac.yaml}` (git-sync DAGs + RBAC to
      launch stage pods) and `pipelines/airflow/dags/idp_medallion_dag.py`
      (`silver >> gold` hourly + daily training DAG)
- [ ] 4.3 `pipelines/spark/gold_job.py` + `deploy/jobs/spark-gold-job.yaml`
      (production Gold via spark-submit)
- [ ] 4.4 `pipeline.run` composes stages locally (dev/test + training)

## 5. Make targets & docs
- [ ] 5.1 `mk-up`, `image`, `deploy`, `infra`, `kafka`, `stream`, `silver`, `gold`,
      `*-fix`, `minio-ls/reset/console`, `kafka-ui`
- [ ] 5.2 Update root README §3 (batch vs streaming) and `deploy/README.md`

## Done when
`specs/platform-deployment` and `specs/orchestration` scenarios pass and the
minikube bring-up (`infra` → `kafka` → `stream` → `silver` → `gold`) works.
