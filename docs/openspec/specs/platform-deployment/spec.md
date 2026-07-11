# Platform Deployment Specification

## Purpose

Deploy the platform on Kubernetes (minikube local, EKS prod) with a clean
namespace split and freely-pullable images.

## Requirements

### Requirement: Namespace split and tenant isolation
The system SHALL deploy platform services into `idp-platform`, application
workloads into `idp-app`, and per-tenant isolation namespaces `idp-tenant-*` with
`ResourceQuota`, a default-deny cross-tenant `NetworkPolicy`, and RBAC roles.
Applications SHALL reach services via cross-namespace DNS.

#### Scenario: App-to-service addressing
- **WHEN** a job in `idp-app` connects to Kafka
- **THEN** it uses `idp-kafka-kafka-bootstrap.idp-platform.svc:9092`

#### Scenario: Cross-tenant traffic blocked
- **WHEN** the isolation manifests are applied
- **THEN** pods in one tenant namespace cannot receive traffic from another tenant

### Requirement: Platform services install
The system SHALL install MinIO, Apache Kafka (Strimzi) + Apicurio registry,
Nessie, Trino, TimescaleDB, Keycloak, Airflow, MLflow, and Prometheus/Grafana via
Helm, using non-Bitnami sources with freely-pullable images (ADR-0017), and SHALL
create per-layer buckets `idp-bronze`/`idp-silver`/`idp-gold`.

#### Scenario: Repos added before install
- **WHEN** the infra install runs
- **THEN** required Helm repos are added first and each component installs
  idempotently

### Requirement: Kafka cluster and topics adapt to the operator
The system SHALL deploy a KRaft Kafka cluster whose API version and Kafka version
are auto-detected from the installed Strimzi operator, and SHALL create per-tenant
topics and register Avro schemas.

#### Scenario: Version auto-detection
- **WHEN** the Kafka setup runs against a given Strimzi operator
- **THEN** it renders the manifests with the CRD's served API version and a
  supported Kafka version, avoiding "unsupported version"/"no matches for kind"
  failures

### Requirement: Overlays and image handling
The system SHALL provide a Kustomize base with `overlays/local` (minikube,
`imagePullPolicy: Never`, image built into the node) and `overlays/aws` (EKS, ECR,
IRSA), plus an Argo CD application for GitOps.

#### Scenario: Local image without registry
- **WHEN** the local overlay is applied after `make image`
- **THEN** the scoring API runs from the locally-built image without pulling a registry
