# Synthetic Data Specification

## Purpose

Provide a deterministic, multi-tenant, labelled data generator so the platform is
trainable and demoable without production data (ADR-0014).

## Requirements

### Requirement: Coherent multi-entity generation
The system SHALL generate referentially-consistent records across all seven TMF
entities (parties, accounts, agreements, products, voice/data usage, recharge)
for one or more tenants, with a subscriber's MSISDN/IMSI/IMEI consistent across
its agreement and events, and every record stamped with `tenant_id`.

#### Scenario: Referential integrity
- **WHEN** a dataset is generated
- **THEN** every usage/recharge `service_id` corresponds to a generated
  agreement MSISDN, and every agreement's `account_id` exists in accounts

#### Scenario: Tenant stamping
- **WHEN** any entity/event table is generated
- **THEN** it contains a `tenant_id` column equal to the requested tenant

### Requirement: Determinism
The system SHALL produce identical output for identical `(tenant_id, seed)`,
using a deterministic seed derivation (not Python's randomized `hash()`).

#### Scenario: Reproducible runs
- **WHEN** the generator runs twice with the same tenant and seed
- **THEN** the label tables are equal and event counts match

### Requirement: Labels for ML
The system SHALL emit a ground-truth label table per subscriber containing
`is_fraud`, `fraud_type`, `will_default`, and `true_credit_limit`, with
configurable base rates, and SHALL inject matching behaviour so the labels are
learnable but not perfectly separable.

#### Scenario: Both classes present with signal
- **WHEN** a dataset of sufficient size is generated
- **THEN** `is_fraud` and `will_default` each contain both classes, and
  SIM-box-fraud subscribers have higher mean voice volume than normal ones
