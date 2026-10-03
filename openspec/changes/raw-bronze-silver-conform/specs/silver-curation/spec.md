# Silver curation delta

## MODIFIED Requirements

### Requirement: REQ-SLV-01 — Canonical Silver tables
The system SHALL produce these canonical Silver tables from Bronze, each carrying
`tenant_id`:

- `dim_subscriber`: one row per subscriber (MSISDN), joining the agreement, its
  customer (`customer_id`), its billing account (`account_id`), the customer's
  `segment` and party attributes, and the product count. It SHALL carry
  `customer_id` and no party id. It SHALL compute and store the customer's tenure,
  `customer_tenure_days`: the whole days elapsed from the customer's `valid_from` to
  the latest customer `valid_from` among the subscribers built in the run, and 0
  when the customer's `valid_from` is unknown.
- `dim_customer`: one row per customer: its party (`party_id`, `party_type`), role,
  `segment`, status and since-date (`valid_from`).
- `dim_billing_account`: one row per billing account: type, lifecycle status,
  `valid_from`, the paying party (`payer_party_id`) and `is_company`.
- `dim_product`: one row per product: its agreement, type, name, category, monthly
  fee and lifecycle status.
- `fact_usage` (unified voice and data), `fact_recharge` and `fact_fee_charge`
  (REQ-SLV-05).
- `quarantine`: the Bronze events that failed conforming (REQ-SLV-09).

#### Scenario: SCN-SLV-01.1 — Build the core tables
- **WHEN** `to_silver` runs on valid Bronze
- **THEN** it returns `dim_subscriber`, `dim_customer`, `dim_billing_account`,
  `dim_product`, `fact_usage`, `fact_recharge` and `fact_fee_charge`, each non-empty
  and tenant-stamped

#### Scenario: SCN-SLV-01.2 — Subscriber grain
- **WHEN** `dim_subscriber` is built
- **THEN** it has one row per `(tenant_id, service_id)` with no duplicates

#### Scenario: SCN-SLV-01.3 — Dimension grains
- **WHEN** `dim_customer`, `dim_billing_account` and `dim_product` are built
- **THEN** each has one row per `(tenant_id, <id>)` (`customer_id`,
  `billing_account_id`, `product_id`) with no duplicates

#### Scenario: SCN-SLV-01.4 — Subscriber keys join the dimensions
- **WHEN** `dim_subscriber` is built
- **THEN** it has a `customer_id` and no `party_id`, every `customer_id` joins
  `dim_customer`, and every `account_id` joins `dim_billing_account`

#### Scenario: SCN-SLV-01.5 — Customer segment and tenure in dim_subscriber
- **WHEN** `dim_subscriber` is built for two subscribers whose customers started
  10.5 days apart, the later one being the latest in the run
- **THEN** each row's `segment` is its customer's, the earlier customer's
  `customer_tenure_days` is 10 and the later one's is 0, and there is no
  `account_segment` or `account_tenure_days` column

### Requirement: REQ-SLV-02 — Id canonicalization and normalization
The Silver stage SHALL:
- strip `UUID(...)` wrappers from entity and relationship ids
- normalize money as `gross_amount = net_amount + tax_amount`
- normalize timestamps to UTC
- de-duplicate on natural keys

Silver first conforms Bronze's raw TMF events to canonical records with the
ingestion adapter (REQ-SLV-09, REQ-BRZ-05), then curates those records.

#### Scenario: SCN-SLV-02.1 — Money normalization
- **WHEN** a usage/recharge record with net + tax amounts is curated
- **THEN** `gross_amount` equals net + tax

#### Scenario: SCN-SLV-02.2 — UUID unwrap from real samples
- **WHEN** a conformed record carries a relationship id wrapped as
  `"UUID(123...)"`, as some operator feeds send it, and is curated
- **THEN** the Silver id is the inner value without the wrapper, so the
  relationship joins its target (e.g. an agreement's `account_id` joins
  `dim_billing_account`)

### Requirement: REQ-SLV-04 — Standalone stage reading Bronze
The Silver stage SHALL always read Bronze from the lake's Iceberg tables, through
the configured catalog (object storage in the cluster, a local warehouse
otherwise). It SHALL NOT accept in-memory input. It SHALL write Silver as Iceberg
tables partitioned by `tenant_id`, and SHALL fail clearly if required entity tables
are missing.

#### Scenario: SCN-SLV-04.1 — Missing Bronze entities
- **WHEN** the Silver stage runs but the raw tables `bronze.tmf_party`,
  `tmf_customer`, `tmf_billing_account` or `tmf_agreement` are absent or empty
- **THEN** it raises a clear error instructing to populate Bronze first

#### Scenario: SCN-SLV-04.2 — Silver reads the Bronze the landing wrote
- **WHEN** Bronze has been landed (by `idp consume` or the local `run`) and the
  Silver stage then runs as a separate process
- **THEN** it builds Silver from exactly that Bronze, read through the catalog

### Requirement: REQ-SLV-05 — Fee-charge fact
The Silver stage SHALL build `fact_fee_charge` from the conformed `fee_charges`
(the raw `bronze.tmf_applied_customer_billing_rate` events): `charged_at` normalized to UTC, tenant-stamped, and de-duplicated on
`(tenant_id, id)`. It SHALL re-derive `product_name`, `category` and the owning
`agreement_id` by joining the conformed products (their latest versions), because
the product owns those attributes. The charge references a product only and carries no PII. The
`agreement_id` lets downstream features attribute a fee to a subscriber.

#### Scenario: SCN-SLV-05.1 — Fee fact built and enriched from products
- **WHEN** Silver is built for a tenant with `fee_charges` in Bronze
- **THEN** `fact_fee_charge` is produced with a UTC `charged_at`, its `product_id`/
  `amount`/`charge_type`, and `product_name`/`category`/`agreement_id` joined from
  `products`, and it contains no raw identifier (no `service_id`/MSISDN)

### Requirement: REQ-SLV-06 — Idempotent Silver writes
Re-running the Silver stage SHALL NOT duplicate or lose rows.
- The dimensions (`dim_subscriber`, `dim_customer`, `dim_billing_account`,
  `dim_product`) are rebuilt and replace the partitions of the tenants present.
- The quarantine is merged on `(tenant_id, source table, event_id)`, so an event
  processed again isn't quarantined twice.
- The event facts are merged on their natural key `(tenant_id, id)`, so a record
  already in Silver is never inserted twice. This holds even when the same Bronze
  records are processed again, for example after a failed run or a full refresh.
- Partitions of tenants absent from the new output SHALL be left untouched.
- Each table's changes SHALL be committed atomically, so a concurrent reader sees
  either the previous or the new state of the table, never a partial write.

#### Scenario: SCN-SLV-06.1 — Re-run does not duplicate
- **WHEN** the Silver stage runs twice on the same Bronze and persists both times
- **THEN** each Silver table has the same row count after the second run as after
  the first, and `dim_subscriber` still has one row per `(tenant_id, service_id)`

#### Scenario: SCN-SLV-06.2 — Other tenants untouched
- **WHEN** Silver is persisted with output for `tenant_a` only, while `tenant_b`
  partitions already exist
- **THEN** the `tenant_a` partitions are replaced and the `tenant_b` partitions are
  unchanged

#### Scenario: SCN-SLV-06.3 — Readers never see a partial write
- **WHEN** a reader (e.g. the Gold stage) reads a Silver table while the Silver stage
  is committing to it
- **THEN** the reader gets a complete snapshot, the one before or the one after the
  commit, and never fails because files disappeared

### Requirement: REQ-SLV-07 — Incremental event facts
The Silver stage SHALL build `fact_usage`, `fact_recharge` and `fact_fee_charge`
incrementally from the raw event tables (`bronze.tmf_product_usage`,
`tmf_topup_balance` and `tmf_applied_customer_billing_rate`), conforming each batch
(REQ-SLV-09) and quarantining its failures in the same commit cycle: each run processes only the Bronze records appended since the
previous successful run, tracked by a durable per-table checkpoint, and merges them
into the existing Silver fact. The cost of a run SHALL therefore scale with the new
Bronze data, not with the total history. A run with no new Bronze data SHALL leave
the facts unchanged. The resulting facts SHALL equal what a full rebuild from all of
Bronze would produce.

#### Scenario: SCN-SLV-07.1 — Only new Bronze is processed
- **WHEN** Silver has processed Bronze up to batch N, and batch N+1 is then landed
  and Silver runs again
- **THEN** only batch N+1's records are read from Bronze, and the facts gain exactly
  batch N+1's new records

#### Scenario: SCN-SLV-07.2 — Nothing new, nothing changes
- **WHEN** Silver runs with no Bronze appended since its previous run
- **THEN** no fact table gains a new snapshot and the row counts are unchanged

#### Scenario: SCN-SLV-07.3 — Incremental equals full rebuild
- **WHEN** Bronze is landed in several batches with Silver running after each, and a
  second lake is built from the same Bronze in one full run
- **THEN** both lakes' Silver fact tables contain the same rows

#### Scenario: SCN-SLV-07.4 — Failure resumes without loss
- **WHEN** a Silver run fails after reading new Bronze but before committing, and
  Silver runs again
- **THEN** the second run processes those records, and none is lost or duplicated

### Requirement: REQ-SLV-08 — Full refresh from Bronze
The Silver stage SHALL offer a full refresh (`idp silver --full-refresh`) that
discards the fact checkpoints and rebuilds every Silver table, the quarantine
included, from all of Bronze.
This is the path for reprocessing history after a curation fix, and the reason Bronze
is kept immutable.

#### Scenario: SCN-SLV-08.1 — Rebuild after a logic change
- **WHEN** `idp silver --full-refresh` runs on a lake whose Silver was built
  incrementally
- **THEN** every Silver table is rebuilt from all Bronze records, and later
  incremental runs continue from the end of Bronze

## ADDED Requirements

### Requirement: REQ-SLV-09 — Conform raw Bronze, quarantine failures
The Silver stage SHALL conform Bronze's raw TMF events to canonical records before
curating them. For each event it SHALL:

- check the event and its resource against the TMF and extension schemas (REQ-BRZ-06)
- check that the resource's `tenantId` equals the event's tenant
- adapt the resource with the active mapping profile (REQ-BRZ-05), resolving its
  references against the tenant's latest customers, parties and billing accounts in
  Bronze
- check the canonical record against its entity's Avro contract (REQ-STR-03)

An event that fails any check SHALL NOT reach a Silver table other than
`silver.quarantine`. Its quarantine row SHALL carry the tenant, the source Bronze
table, the `event_id`, the resource type and id, the reason, and the time it was
quarantined. Quarantining SHALL NOT fail the Silver run. The run SHALL report the
number of quarantined events per source table. Because Bronze keeps the event, a
full refresh (REQ-SLV-08) after a fix conforms it again.

#### Scenario: SCN-SLV-09.1 — Conformed records equal the generated ones
- **WHEN** a generated dataset is published, consumed and conformed
- **THEN** the conformed records equal the dataset's canonical records exactly, per
  tenant and entity, and the quarantine is empty

#### Scenario: SCN-SLV-09.2 — Invalid, foreign and dangling events quarantined
- **WHEN** Bronze holds a resource that fails its schema, one whose `tenantId` is
  another tenant, and an agreement whose customer doesn't exist
- **THEN** each is in `silver.quarantine` with its reason, none of them reaches
  another Silver table, and the run succeeds and reports them

#### Scenario: SCN-SLV-09.3 — Contract violation quarantined
- **WHEN** an event adapts to a record that breaks its canonical contract (e.g. a
  `business_line` outside the contract's symbols)
- **THEN** it is quarantined with the contract error

#### Scenario: SCN-SLV-09.4 — Fixed and reprocessed
- **WHEN** quarantined events become valid after a profile fix, and
  `idp silver --full-refresh` runs
- **THEN** they are conformed into Silver and are no longer in the quarantine

### Requirement: REQ-SLV-10 — Latest version of reference data
For the reference entities (parties, customers, billing accounts, agreements,
products), the Silver stage SHALL use, per `(tenant_id, id)`, the version with the
latest `event_time`; between events with the same `event_time`, the one landed last
(Bronze `_ingested_at`, then Kafka offset). An older version SHALL NOT appear in
Silver, whatever order Bronze holds them in. Event facts (usage, recharges, fee
charges) are immutable: Silver keeps the first conformed event per `(tenant_id, id)`,
as REQ-SLV-06 merges them.

#### Scenario: SCN-SLV-10.1 — Updated party wins
- **WHEN** Bronze holds two events for the same party, the later one changing its
  language
- **THEN** `dim_subscriber` and `dim_customer` reflect the later version only

#### Scenario: SCN-SLV-10.2 — Order in Bronze doesn't matter
- **WHEN** the later version of a billing account was landed before the earlier one
- **THEN** Silver still uses the version with the later `event_time`
