# Synthetic Data Specification

## Purpose

Provide a deterministic, multi-tenant, labelled data generator so the platform is
trainable and demoable without production data (ADR-0014).

## Requirements

### Requirement: REQ-SYN-01 — Coherent multi-entity generation
The system SHALL generate referentially-consistent records across all seven TMF
entities (parties, accounts, agreements, products, voice/data usage, recharge)
for one or more tenants, with a subscriber's MSISDN/IMSI/IMEI consistent across
its agreement and events, and every record stamped with `tenant_id`.

#### Scenario: SCN-SYN-01.1 — Referential integrity
- **WHEN** a dataset is generated
- **THEN** every usage/recharge `service_id` corresponds to a generated
  agreement MSISDN, and every agreement's `account_id` exists in accounts

#### Scenario: SCN-SYN-01.2 — Tenant stamping
- **WHEN** any entity/event table is generated
- **THEN** it contains a `tenant_id` column equal to the requested tenant

### Requirement: REQ-SYN-02 — Determinism
The system SHALL produce identical output for identical `(tenant_id, seed)`,
using a deterministic seed derivation (not Python's randomized `hash()`).

#### Scenario: SCN-SYN-02.1 — Reproducible runs
- **WHEN** the generator runs twice with the same tenant and seed
- **THEN** the label tables are equal and event counts match

### Requirement: REQ-SYN-03 — Labels for ML
The system SHALL emit a ground-truth label table per subscriber containing
`is_fraud`, `fraud_type`, `will_default`, and `true_credit_limit`, with
configurable base rates, and SHALL inject matching behaviour so the labels are
learnable but not perfectly separable.

#### Scenario: SCN-SYN-03.1 — Both classes present with signal
- **WHEN** a dataset of sufficient size is generated
- **THEN** `is_fraud` and `will_default` each contain both classes, and
  SIM-box-fraud subscribers have higher mean voice volume than normal ones

### Requirement: REQ-SYN-04 — Product fee charges & catalog enrichment
The generator SHALL additionally emit a `fee_charges` fact (product/subscription
fees) with `charge_type` ∈ {`recurring`,`one-time`,`overage`,`discount`} and a
`category`, referentially tied to a subscriber's products; enrich `products` with
`category` and `monthly_fee`; and draw `recharge_type` from a realistic mix rather
than a constant. These additions SHALL be additive (no change to labels, features,
or the existing entity columns) and deterministic under `(tenant_id, seed)`.

#### Scenario: SCN-SYN-04.1 — Fee charges are coherent and typed
- **WHEN** a dataset is generated
- **THEN** every `fee_charges` row carries a `tenant_id`, a `product_id`/
  `agreement_id` that exist in the generated products/agreements, an `amount`, and
  a `charge_type`; recurring charges use the product's `monthly_fee`

#### Scenario: SCN-SYN-04.2 — Non-degenerate recharge types
- **WHEN** recharges are generated for a sufficiently large dataset
- **THEN** `recharge_type` takes more than one distinct value
