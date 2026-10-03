# Synthetic Data Specification

## Purpose

Provide a deterministic, multi-tenant, labelled data generator so the platform is
trainable and demoable without production data (ADR-0014).

## Requirements

### Requirement: REQ-SYN-01 — Coherent multi-entity generation
The system SHALL generate referentially-consistent records across the canonical
entities (parties, customers, billing accounts, agreements, products, voice/data
usage, recharge) for one or more tenants, with a subscriber's MSISDN/IMSI/IMEI
consistent across its agreement and events, and every record stamped with
`tenant_id`.

Each billing account SHALL reference its paying party (`payer_party_id`). Each
billing account SHALL have one customer: the party role through which the paying
party holds the account and its agreements, carrying the party (`party_id`), its
role, `segment`, status and since-date, and the billing account (`account_id`). The
segment belongs to the customer: billing accounts SHALL NOT carry one. Each agreement
SHALL carry its customer (`customer_id`) and that customer's billing account
(`account_id`). The parties SHALL NOT carry a role or an account: those belong to
the customer.

The payer party of a company billing account (`is_company`) SHALL be an
organization: its `party_type` is `organization`, it carries an organization name
and type, and its person-only fields (`first_name`, `last_name`, `date_of_birth`) are
null. Every other party SHALL be an individual (`party_type = individual`).
Introducing customers and renaming accounts to billing accounts SHALL NOT change
any generated value: they reorganize the same values.

#### Scenario: SCN-SYN-01.1 — Referential integrity
- **WHEN** a dataset is generated
- **THEN** every usage/recharge `service_id` corresponds to a generated agreement
  MSISDN; every agreement's `account_id` exists in billing accounts and its
  `customer_id` in customers; and every billing account's `payer_party_id` and every
  customer's `party_id` exist in parties

#### Scenario: SCN-SYN-01.2 — Tenant stamping
- **WHEN** any entity/event table is generated
- **THEN** it contains a `tenant_id` column equal to the requested tenant

#### Scenario: SCN-SYN-01.3 — Company accounts have organization parties
- **WHEN** a dataset is generated
- **THEN** the payer party of every billing account with `is_company = true` has
  `party_type = organization`, a non-empty organization name, and null
  `first_name`, `last_name` and `date_of_birth`; every other party has
  `party_type = individual` and its person fields set; and the values of the former
  model are unchanged: each billing account equals the former account (with
  `party_id` as `payer_party_id`) apart from its segment, which is its customer's
  `segment`, and each party's former role and account are its customer's

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
fees) with `charge_type` ∈ {`recurring`,`one_time`,`overage`,`discount`}, associated
with a **product only** (a `product_id` FK — name/category live on the product, the
service on its agreement, so they are NOT denormalized onto the charge); enrich
`products` with `category` and `monthly_fee`; and draw `recharge_type` from a
realistic mix rather than a constant. These additions SHALL be additive (no change
to labels, features, or the existing entity columns) and deterministic under
`(tenant_id, seed)`.

#### Scenario: SCN-SYN-04.1 — Fee charges are coherent and typed
- **WHEN** a dataset is generated
- **THEN** every `fee_charges` row carries a `tenant_id`, a `product_id` that exists
  in the generated products, an `amount`, and a `charge_type`, and carries no
  denormalized `service_id`/`product_name`/`category`; recurring charges use the
  product's `monthly_fee`

#### Scenario: SCN-SYN-04.2 — Non-degenerate recharge types
- **WHEN** recharges are generated for a sufficiently large dataset
- **THEN** `recharge_type` takes more than one distinct value
