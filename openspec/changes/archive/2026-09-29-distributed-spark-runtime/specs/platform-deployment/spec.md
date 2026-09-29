# Spec Delta

## ADDED Requirements

### Requirement: REQ-PLT-10 — Spark Operator
The platform SHALL install the Kubeflow Spark Operator in `idp-platform`, watching
`idp-app`, so that Spark stages run as `SparkApplication`s with separate driver and
executor pods. The pods use the `idp` image and a dedicated Spark ServiceAccount in
`idp-app`, whose RBAC lets the driver manage its own executor pods. The Airflow
ServiceAccount SHALL be allowed to create, watch and delete `SparkApplication`s in
`idp-app`. The install SHALL be idempotent.

#### Scenario: SCN-PLT-10.1 — Operator runs Spark applications
- **WHEN** a `SparkApplication` for the Gold stage is applied in `idp-app`
- **THEN** the operator starts a driver pod, which starts its executors, and the
  application reaches `COMPLETED` with Gold written

#### Scenario: SCN-PLT-10.2 — Airflow can submit
- **WHEN** an Airflow Spark task runs
- **THEN** it creates the `SparkApplication` in `idp-app` with its own
  ServiceAccount, and reports the application's final state
