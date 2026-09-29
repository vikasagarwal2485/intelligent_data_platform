# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-ORC-05 — Single transformation engine; pandas confined to ML
The Silver and Gold transforms SHALL have exactly one implementation, on Apache
Spark, shared by the CLI stages, the cluster jobs, the Airflow tasks and the tests.
The execution environment SHALL only select the Spark master and its resources:
- `local[*]` by default (local runs and CI)
- Kubernetes through the Spark Operator in the cluster
- overridable by configuration

No stage SHALL materialize a whole lake table into single-process (pandas) memory.
pandas SHALL be used only:
- per partition, inside distributed scoring
- for model training, on a bounded sample (REQ-ML-09)
- by the synthetic data generator
- by the Python Bronze landing, per consumed batch
- to shape the bounded results of analytics queries (REQ-MCP-09)

#### Scenario: SCN-ORC-05.1 — Same code in every entrypoint
- **WHEN** Silver is built once with `idp silver` locally and once by the Silver
  Spark application in the cluster, from equal Bronze
- **THEN** both run the same transform code and produce equal Silver tables; only
  the Spark master and resources differ

#### Scenario: SCN-ORC-05.2 — Master from configuration
- **WHEN** no Spark master is configured
- **THEN** the stages run on `local[*]`; **AND WHEN** a master is configured
  (e.g. via `IDP_SPARK_MASTER`) **THEN** the stages use it

#### Scenario: SCN-ORC-05.3 — No whole-table pandas in Silver/Gold
- **WHEN** the Silver, Gold, Score or publish stage runs
- **THEN** it completes without converting a whole input or output table to a
  pandas DataFrame
