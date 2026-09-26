# Spec Delta

## MODIFIED Requirements

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
