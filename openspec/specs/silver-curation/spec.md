# Silver Curation Specification

## Purpose

Turn raw per-entity Bronze records into clean, canonical, joinable, PII-safe
tables. Silver is a standalone stage that reads Bronze and writes Silver.

## Requirements

### Requirement: REQ-SLV-01 — Canonical Silver tables
The system SHALL produce four canonical tables from Bronze: `dim_subscriber`
(one row per subscriber joining party/account/agreement/products), `fact_usage`
(unified voice+data), `fact_recharge`, and `fact_fee_charge` (REQ-SLV-05), each
carrying `tenant_id`.

#### Scenario: SCN-SLV-01.1 — Build the core tables
- **WHEN** `to_silver` runs on valid Bronze
- **THEN** it returns `dim_subscriber`, `fact_usage`, `fact_recharge` and
  `fact_fee_charge`, each non-empty and tenant-stamped

#### Scenario: SCN-SLV-01.2 — Subscriber grain
- **WHEN** `dim_subscriber` is built
- **THEN** it has one row per `(tenant_id, service_id)` with no duplicates

### Requirement: REQ-SLV-02 — Id canonicalization and normalization
The Silver stage SHALL:
- strip `UUID(...)` wrappers from entity and relationship ids
- normalize money as `gross_amount = net_amount + tax_amount`
- normalize timestamps to UTC
- de-duplicate on natural keys

Silver curates canonical Bronze records; it does not parse JSON:API envelopes.
Unwrapping envelopes into canonical records is the ingestion adapter's job
(REQ-BRZ-04).

#### Scenario: SCN-SLV-02.1 — Money normalization
- **WHEN** a usage/recharge record with net + tax amounts is curated
- **THEN** `gross_amount` equals net + tax

#### Scenario: SCN-SLV-02.2 — UUID unwrap from real samples
- **WHEN** a Bronze record carries a relationship id wrapped as `"UUID(123...)"`,
  as in the real TM Forum documents in `data_samples/`, and is curated
- **THEN** the Silver id is the inner value without the wrapper, so the
  relationship joins its target (e.g. an agreement's `account_id` joins
  `accounts`)

### Requirement: REQ-SLV-03 — PII tokenization at Silver
The Silver stage SHALL tokenize PII columns per tenant so raw identifiers never
appear in Silver, while keeping tokens consistent across `dim_subscriber` and the
fact tables to allow joins.

#### Scenario: SCN-SLV-03.1 — Tokens join across tables
- **WHEN** Silver is built for a tenant
- **THEN** a subscriber's `service_id` token in `dim_subscriber` equals its token
  in `fact_usage`/`fact_recharge`, and raw MSISDNs are absent

### Requirement: REQ-SLV-04 — Standalone stage reading Bronze
The Silver stage SHALL always read Bronze from the lake: object storage when
configured, otherwise the local lake directory. It SHALL NOT accept in-memory
input. It SHALL write Silver partitioned by `tenant_id`, and SHALL fail clearly if
required entity tables are missing.

#### Scenario: SCN-SLV-04.1 — Missing Bronze entities
- **WHEN** the Silver stage runs but `parties`/`accounts`/`agreements` are absent
  or empty in Bronze
- **THEN** it raises a clear error instructing to populate Bronze first

#### Scenario: SCN-SLV-04.2 — Silver reads the Bronze the landing wrote
- **WHEN** Bronze has been landed (by `idp consume` or the local `run`) and the
  Silver stage then runs as a separate process
- **THEN** it builds Silver from exactly that Bronze, read from the configured lake
  location

### Requirement: REQ-SLV-05 — Fee-charge fact
The Silver stage SHALL build `fact_fee_charge` from the `fee_charges` Bronze
entity: `charged_at` normalized to UTC, tenant-stamped, and de-duplicated on
`(tenant_id, id)`. It SHALL re-derive `product_name`, `category` and the owning
`agreement_id` by joining the `products` table, because the product owns those
attributes. The charge references a product only and carries no PII. The
`agreement_id` lets downstream features attribute a fee to a subscriber.

#### Scenario: SCN-SLV-05.1 — Fee fact built and enriched from products
- **WHEN** Silver is built for a tenant with `fee_charges` in Bronze
- **THEN** `fact_fee_charge` is produced with a UTC `charged_at`, its `product_id`/
  `amount`/`charge_type`, and `product_name`/`category`/`agreement_id` joined from
  `products`, and it contains no raw identifier (no `service_id`/MSISDN)

### Requirement: REQ-SLV-06 — Idempotent Silver writes
Persisting Silver SHALL replace, for each table, the partitions of the tenants
present in the new output. Re-running the Silver stage on unchanged Bronze SHALL
leave every Silver table with identical contents. Partitions of tenants absent from
the new output SHALL be left untouched.

#### Scenario: SCN-SLV-06.1 — Re-run does not duplicate
- **WHEN** the Silver stage runs twice on the same Bronze and persists both times
- **THEN** each Silver table has the same row count after the second run as after
  the first, and `dim_subscriber` still has one row per `(tenant_id, service_id)`

#### Scenario: SCN-SLV-06.2 — Other tenants untouched
- **WHEN** Silver is persisted with output for `tenant_a` only, while `tenant_b`
  partitions already exist
- **THEN** the `tenant_a` partitions are replaced and the `tenant_b` partitions are
  unchanged
