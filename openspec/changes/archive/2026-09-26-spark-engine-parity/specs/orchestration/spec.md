# Spec Delta

## ADDED Requirements

### Requirement: REQ-ORC-04 — Engine output parity (pandas ↔ Spark)
For the same input layer, the Spark implementation of a stage SHALL produce the
same tables, with the same column names, order and logical types, as the pandas
implementation. Specifically:

- **Spark Silver** SHALL produce `dim_subscriber`, `fact_usage`, `fact_recharge` and
  `fact_fee_charge` (REQ-SLV-01/05), with identical tokenized identifiers.
- **Spark Gold** SHALL produce `credit_features` and `fraud_features` with the
  `(tenant_id, service_id)` keys followed by the full credit and fraud model feature
  sets, using the configured lookback windows.

Integer-valued columns (counts, days) SHALL match exactly. Real-valued columns SHALL
match within an absolute tolerance of 1e-6 (rounded columns: within their rounding
unit). Spark writes SHALL replace only the partitions of the tenants being written.

#### Scenario: SCN-ORC-04.1 — Silver parity
- **WHEN** the pandas and Spark Silver stages run on the same Bronze with the same
  tenant salts
- **THEN** both produce the four Silver tables with identical schemas, identical
  `service_id` tokens, and equal row counts per tenant

#### Scenario: SCN-ORC-04.2 — Gold parity
- **WHEN** the pandas and Spark Gold stages run on the same Silver
- **THEN** both `credit_features` tables have the same columns in the same order,
  one row per `(tenant_id, service_id)`, and matching values within tolerance, and
  likewise for `fraud_features`

#### Scenario: SCN-ORC-04.3 — Spark-built Gold is scoreable
- **WHEN** Gold was produced by the Spark engine and `idp score` then runs with
  trained models
- **THEN** scoring succeeds and writes `credit_scores` and `fraud_scores`

#### Scenario: SCN-ORC-04.4 — Spark replaces per tenant
- **WHEN** a Spark stage writes output containing only `tenant_a`
- **THEN** the `tenant_b` partitions of that table are unchanged
