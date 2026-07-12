# Silver Curation Specification

## Purpose

Turn raw per-entity Bronze records into clean, canonical, joinable, PII-safe
tables. Silver is a standalone stage that reads Bronze and writes Silver.

## Requirements

### Requirement: REQ-SLV-01 — Canonical Silver tables
The system SHALL produce three canonical tables from Bronze: `dim_subscriber`
(one row per subscriber joining party/account/agreement/products), `fact_usage`
(unified voice+data), and `fact_recharge`, each carrying `tenant_id`.

#### Scenario: SCN-SLV-01.1 — Build all three tables
- **WHEN** `to_silver` runs on valid Bronze
- **THEN** it returns exactly `dim_subscriber`, `fact_usage`, `fact_recharge`,
  each non-empty and tenant-stamped

#### Scenario: SCN-SLV-01.2 — Subscriber grain
- **WHEN** `dim_subscriber` is built
- **THEN** it has one row per `(tenant_id, service_id)` with no duplicates

### Requirement: REQ-SLV-02 — JSON:API unwrap and normalization
The system SHALL unwrap JSON:API envelopes, strip `UUID(...)` wrappers from
relationship ids, normalize money as `gross_amount = net_amount + tax_amount`,
normalize timestamps to UTC, and deduplicate on natural keys.

#### Scenario: SCN-SLV-02.1 — Money normalization
- **WHEN** a usage/recharge record with net + tax amounts is curated
- **THEN** `gross_amount` equals net + tax

#### Scenario: SCN-SLV-02.2 — UUID unwrap from real samples
- **WHEN** a `data_samples/` JSON:API document with `"UUID(123...)"` is unwrapped
- **THEN** the resolved relationship id is the inner value without the wrapper

### Requirement: REQ-SLV-03 — PII tokenization at Silver
The Silver stage SHALL tokenize PII columns per tenant so raw identifiers never
appear in Silver, while keeping tokens consistent across `dim_subscriber` and the
fact tables to allow joins.

#### Scenario: SCN-SLV-03.1 — Tokens join across tables
- **WHEN** Silver is built for a tenant
- **THEN** a subscriber's `service_id` token in `dim_subscriber` equals its token
  in `fact_usage`/`fact_recharge`, and raw MSISDNs are absent

### Requirement: REQ-SLV-04 — Standalone stage reading Bronze
`to_silver` SHALL read Bronze from storage when not given in-memory frames, and
SHALL write Silver partitioned by `tenant_id`; it SHALL fail clearly if required
entity tables are missing.

#### Scenario: SCN-SLV-04.1 — Missing Bronze entities
- **WHEN** `to_silver` runs but `parties`/`accounts`/`agreements` are absent/empty
- **THEN** it raises a clear error instructing to populate Bronze first
