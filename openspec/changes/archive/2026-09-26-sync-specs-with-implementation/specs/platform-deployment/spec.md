# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-PLT-02 — Platform services install
The system SHALL install MinIO, Apache Kafka (Strimzi) + Apicurio registry, Nessie,
Trino, TimescaleDB, Keycloak, MLflow, and Prometheus/Grafana. These SHALL come from
Helm charts or plain manifests using non-Bitnami sources with freely-pullable images
(ADR-0017). The install SHALL create the lake buckets
`idp-bronze`/`idp-silver`/`idp-gold` and the model/labels bucket `idp-ml`. Bucket
creation SHALL be idempotent. Airflow SHALL be installed by a separate step
(`make airflow-wire`) that pins the chart version and uses TimescaleDB as an
external metadata database. An optional Kafka admin UI SHALL be deployable
(`make kafka-ui`). Kafka topics SHALL carry a delete-based retention, 7 days by
default and configurable.

#### Scenario: SCN-PLT-02.1 — Repos added before install
- **WHEN** the infra install runs
- **THEN** required Helm repos are added first and each component installs
  idempotently

#### Scenario: SCN-PLT-02.2 — Buckets present after install
- **WHEN** the infra install (or `make minio-mkbuckets`) completes
- **THEN** buckets `idp-bronze`, `idp-silver`, `idp-gold` and `idp-ml` exist, and
  re-running does not fail

#### Scenario: SCN-PLT-02.3 — Airflow installed separately
- **WHEN** `make airflow-wire` runs after the platform services are up
- **THEN** Airflow is installed at the pinned chart version with its metadata DB on
  TimescaleDB, and the three IDP DAGs are loaded
