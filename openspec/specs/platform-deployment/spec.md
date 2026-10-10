# Platform Deployment Specification

## Purpose

Deploy the platform on Kubernetes (minikube local, EKS prod) with a clean
namespace split and freely-pullable images.

## Requirements

### Requirement: REQ-PLT-01 — Namespace split and tenant isolation
The system SHALL deploy platform services into `idp-platform`, application
workloads into `idp-app`, and per-tenant isolation namespaces `idp-tenant-*` with
`ResourceQuota`, a default-deny cross-tenant `NetworkPolicy`, and RBAC roles.
Applications SHALL reach services via cross-namespace DNS.

#### Scenario: SCN-PLT-01.1 — App-to-service addressing
- **WHEN** a job in `idp-app` connects to Kafka
- **THEN** it uses `idp-kafka-kafka-bootstrap.idp-platform.svc:9092`

#### Scenario: SCN-PLT-01.2 — Cross-tenant traffic blocked
- **WHEN** the isolation manifests are applied
- **THEN** pods in one tenant namespace cannot receive traffic from another tenant

### Requirement: REQ-PLT-02 — Platform services install
The system SHALL install MinIO, Apache Kafka (Strimzi) + Apicurio registry, Nessie,
Trino, TimescaleDB, Keycloak, MLflow, and Prometheus/Grafana. These SHALL come from
Helm charts or plain manifests using non-Bitnami sources with freely-pullable images
(ADR-0017). The install SHALL create the lake buckets
`idp-bronze`/`idp-silver`/`idp-gold`, the model/labels bucket `idp-ml`, and the
source landing bucket `idp-landing`. Bucket creation SHALL be idempotent. Airflow SHALL be installed by a separate step
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
- **THEN** buckets `idp-bronze`, `idp-silver`, `idp-gold`, `idp-ml` and
  `idp-landing` exist, and re-running does not fail

#### Scenario: SCN-PLT-02.3 — Airflow installed separately
- **WHEN** `make airflow-wire` runs after the platform services are up
- **THEN** Airflow is installed at the pinned chart version with its metadata DB on
  TimescaleDB, and the four IDP DAGs are loaded

### Requirement: REQ-PLT-03 — Kafka cluster and topics adapt to the operator
The system SHALL deploy a KRaft Kafka cluster whose API version and Kafka version
are auto-detected from the installed Strimzi operator, and SHALL create per-tenant
topics and register Avro schemas.

#### Scenario: SCN-PLT-03.1 — Version auto-detection
- **WHEN** the Kafka setup runs against a given Strimzi operator
- **THEN** it renders the manifests with the CRD's served API version and a
  supported Kafka version, avoiding "unsupported version"/"no matches for kind"
  failures

### Requirement: REQ-PLT-04 — Overlays and image handling
The system SHALL provide a Kustomize base with `overlays/local` (minikube,
`imagePullPolicy: Never`, image built into the node) and `overlays/aws` (EKS, ECR,
IRSA), plus an Argo CD application for GitOps.

#### Scenario: SCN-PLT-04.1 — Local image without registry
- **WHEN** the local overlay is applied after `make image`
- **THEN** the scoring API runs from the locally-built image without pulling a registry

### Requirement: REQ-PLT-05 — Cluster metrics exporters
_Added: 2026-07-14._
The system SHALL deploy the Prometheus node exporter and kube-state-metrics (via
the kube-prometheus-stack) so that node/host metrics and Kubernetes object state
are scraped by Prometheus and available for dashboards.

#### Scenario: SCN-PLT-05.1 — Exporters scraped by Prometheus
- **WHEN** the monitoring stack is installed on minikube
- **THEN** Prometheus has `up` targets for the node exporter and kube-state-metrics
  and exposes `node_*` and `kube_*` series

### Requirement: REQ-PLT-06 — Grafana service
_Added: 2026-07-14._
The system SHALL deploy Grafana (via the kube-prometheus-stack) with the in-cluster
Prometheus pre-provisioned as the default datasource and a dashboard sidecar that
auto-loads dashboards from labelled ConfigMaps. Grafana SHALL be reachable for the
operator (port-forward in local, ingress in prod).

#### Scenario: SCN-PLT-06.1 — Grafana reachable with Prometheus datasource
- **WHEN** the operator runs `make grafana-ui` and logs in
- **THEN** Grafana is served on the forwarded port and its default Prometheus
  datasource returns data

### Requirement: REQ-PLT-07 — Kubernetes cluster dashboard in Grafana
_Added: 2026-07-14._
The system SHALL present Kubernetes/minikube cluster metrics (node CPU, memory,
running pods) as a Grafana dashboard — the stack's default Kubernetes dashboards
plus an "IDP — Kubernetes Cluster Overview" dashboard provisioned via a labelled
ConfigMap.

#### Scenario: SCN-PLT-07.1 — Cluster dashboard renders
- **WHEN** Grafana is open with the exporters scraping
- **THEN** the cluster dashboard shows node CPU%/memory% and a running-pods count

### Requirement: REQ-PLT-08 — Kafka produce/consume dashboard in Grafana
_Added: 2026-07-14._
The system SHALL expose Kafka broker JMX metrics (via the Strimzi JMX Prometheus
exporter) and consumer-group lag (via the Strimzi Kafka Exporter), scrape them
with a `PodMonitor`, and present a Grafana dashboard showing produce throughput
(messages/bytes in), consume throughput (bytes out), and per-group/topic lag.

#### Scenario: SCN-PLT-08.1 — Produce/consume visible after streaming
- **WHEN** `make stream` produces and consumes records
- **THEN** the Kafka dashboard shows non-zero messages-in and bytes-in/out rates
  and consumer-group lag drains toward zero as the consumer catches up

### Requirement: REQ-PLT-09 — Persistent Iceberg catalog (JDBC on the platform Postgres)
The platform SHALL provide the lake's Iceberg catalog as an Iceberg JDBC catalog
stored in the in-cluster TimescaleDB/Postgres, database `iceberg`, created
idempotently by the platform install. The stage jobs, the scoring API and the DAG
pods SHALL reach it with the catalog env (`IDP_CATALOG_TYPE=sql`, `IDP_CATALOG_URI`
set to that database). The catalog SHALL keep every table's full snapshot history,
because the incremental Silver checkpoints reference Bronze snapshots by id. Table
metadata and data SHALL stay in the bucket of their layer (`idp-bronze`,
`idp-silver`, `idp-gold`). Restarting any catalog client SHALL NOT lose any table,
namespace or snapshot.

#### Scenario: SCN-PLT-09.1 — Catalog state is durable
- **WHEN** the lake has tables and the pods that use the catalog are restarted
- **THEN** every namespace, table and snapshot is still listed, and the stages read
  and write them unchanged

#### Scenario: SCN-PLT-09.2 — Catalog reachable from the stages
- **WHEN** a stage job in `idp-app` starts with the catalog env
- **THEN** it can create, read and commit tables in `bronze`, `silver` and `gold`,
  from both Spark and the Python readers and writers

#### Scenario: SCN-PLT-09.3 — Snapshot history is kept
- **WHEN** a Bronze table receives several appends between two Silver runs
- **THEN** the catalog lists one snapshot per append, and the next incremental Silver
  run resumes from its checkpoint and reads exactly the appended records

### Requirement: REQ-PLT-10 — Spark Operator
The platform SHALL install the Kubeflow Spark Operator in `idp-platform`, watching
`idp-app`, so that Spark stages run as `SparkApplication`s with separate driver and
executor pods. The pods use the `idp` image and a dedicated Spark ServiceAccount in
`idp-app`, whose RBAC lets the driver manage its own executor pods. The Airflow
ServiceAccount SHALL be allowed to create, watch and delete `SparkApplication`s in
`idp-app`, and to read pods and pod logs there so it can follow a driver. It SHALL
NOT be allowed to create, delete or exec into pods in `idp-app`: every Airflow task
is a Spark application (REQ-ORC-02). The install SHALL be idempotent.

#### Scenario: SCN-PLT-10.1 — Operator runs Spark applications
- **WHEN** a `SparkApplication` for the Gold stage is applied in `idp-app`
- **THEN** the operator starts a driver pod, which starts its executors, and the
  application reaches `COMPLETED` with Gold written

#### Scenario: SCN-PLT-10.2 — Airflow can submit
- **WHEN** an Airflow Spark task runs
- **THEN** it creates the `SparkApplication` in `idp-app` with its own
  ServiceAccount, and reports the application's final state

#### Scenario: SCN-PLT-10.3 — Airflow cannot start pods
- **WHEN** Airflow's ServiceAccount is checked for creating a pod in `idp-app`
- **THEN** the request is denied, while creating a `SparkApplication` and reading a
  driver pod's logs are allowed

### Requirement: REQ-PLT-11 — Trino lake catalog and read-only analytics identity
Trino SHALL expose an Iceberg catalog `lake` backed by the lake's Iceberg JDBC
catalog (the `iceberg` database on the platform Postgres, catalog name `lake`, the
same rows the stages commit to), with native S3 access to the layer buckets, so that the `bronze`,
`silver` and `gold` namespaces are queryable as schemas.

Trino SHALL authenticate every client request; a request carrying only a user name
(no credential) SHALL be rejected. It SHALL accept:
- **access tokens** issued by the platform realm, validated for signature (the
  realm's published keys), issuer, audience `trino` and expiry, the user being the
  token's `idp_principal` claim;
- **passwords** for the service users `admin` (operations) and `superset` (the BI
  tool) only, kept in a Kubernetes Secret.

Clients SHALL connect over HTTPS. Analytics identities are tenant principals whose
access is read-only and row-filtered (REQ-TEN-03): `SELECT` on `lake.silver` and
`lake.gold` only, and everything else denied, including all writes and DDL, with
one exception for client metadata: read-only access to `system.jdbc` and
`system.metadata`, whose rows Trino filters by the principal's table access, so
BI clients can validate tables (never `system.runtime` or any other `system`
schema). There is no shared analytics identity. Every user other than `admin`
SHALL see and kill only their own queries: the query list of the REST API and
`system.runtime.queries` never show another user's query text. Trino SHALL be reachable in-cluster at
`trino.idp-platform.svc:8443`, and from the operator's host through a port-forward
target.

#### Scenario: SCN-PLT-11.1 — Lake tables visible in Trino
- **WHEN** the lake has Silver and Gold tables and `admin` lists and reads them
- **THEN** `SHOW TABLES FROM lake.silver` lists the Silver tables, and a `SELECT` on
  one returns the rows the stages committed

#### Scenario: SCN-PLT-11.2 — Analytics identity is read-only
- **WHEN** a tenant principal runs a `SELECT` on `lake.gold.credit_scores`, and then an
  `INSERT` or a `SELECT` on `lake.bronze.tmf_party`
- **THEN** the first succeeds, and the others are denied by access control

#### Scenario: SCN-PLT-11.3 — A user name alone is not accepted
- **WHEN** a client sends a query with only a user name (e.g. `X-Trino-User: admin`),
  or a wrong password for `admin`
- **THEN** Trino rejects it as unauthenticated

#### Scenario: SCN-PLT-11.4 — Only HTTPS carries credentials
- **WHEN** a client sends a token or password to Trino over plain HTTP
- **THEN** the request is refused, and the same request over HTTPS is accepted

#### Scenario: SCN-PLT-11.5 — Only your own queries are visible
- **WHEN** `bob@tenant_b` has run a query and `alice@tenant_a` lists the queries
  through the REST API (`GET /v1/query`)
- **THEN** she sees only her own queries, never Bob's query text; `admin` sees all

#### Scenario: SCN-PLT-11.6 — Client metadata, filtered
- **WHEN** a tenant principal reads `system.jdbc.tables` and `system.metadata`, and then
  `system.runtime.queries`
- **THEN** the first two list only `lake.silver` and `lake.gold` tables (no Bronze), and
  the third is denied

### Requirement: REQ-PLT-12 — Apache Superset service
The platform SHALL deploy Apache Superset in `idp-platform` through the Apache
Superset Kubernetes Operator (pinned), as one `Superset` resource, with a pinned
image that adds the Trino and Postgres drivers and the OIDC client, with:
- its metadata in a `superset` database on the platform TimescaleDB, created if
  missing, and migrated whenever the image changes;
- no bundled Postgres or Redis, and no Celery workers or beat;
- its secret key, the OIDC client secret, the `superset` Trino password and the
  tenant roles' passwords from Kubernetes Secrets, never from the values file or the
  image;
- the sign-in configuration of REQ-BI-01, and both database connections of
  REQ-BI-05.

Superset SHALL be reachable from the operator's host through a port-forward target,
and the documented host setup SHALL complete the Keycloak sign-in in a browser.

#### Scenario: SCN-PLT-12.1 — Installed and reachable
- **WHEN** the Superset target runs on a cluster with the platform services
- **THEN** Superset becomes ready, its metadata lives in the `superset` database, and
  `http://localhost:8088` offers the Keycloak sign-in

#### Scenario: SCN-PLT-12.2 — No secret in the repository
- **WHEN** the deployment files and the image are searched for the secret key, the
  `superset` Trino password and the database passwords
- **THEN** none is found; they come from Secrets. The only credential in the
  repository is the realm's documented dev-default `idp-superset` client secret, which
  `IDP_KC_SECRET_IDP_SUPERSET` replaces, as for every realm client
