# Design: Raw Bronze, canonical Silver

## Context

See proposal.md for why. The current state:

- **Consumer** (`streaming/consumer.py`):
  - reads each tenant's compacted customer topic in full (`read_customers`), then
    drains the other topics;
  - per message: `from_event` (envelope, schema, `tenantId`), `adapt_resource` with
    the customer index, then `contracts.error`;
  - lands canonical frames with `pipeline.to_bronze` (`bronze.<entity>`,
    `(tenant_id, ingest_date)`), and dead-letters everything else.
- **Silver** (`transforms/incremental.py`, `silver.py`):
  - `build_dim` rebuilds `dim_subscriber` in full from `REFERENCE = parties,
    accounts, agreements, products`;
  - `process_facts` streams each fact's Bronze sources (`voice_events`,
    `data_events`, `recharge_events`, `fee_charges`) with `Trigger.Once` and a
    checkpoint, and merges insert-if-absent on `(tenant_id, id)`.
- **Mapping profile** (`ingestion/tmf/profile.py`, `standard.yaml`):
  - Customer is a special section, built by `to_tmf` from the accounts and resolved
    by `_Customers`;
  - an account's segment, `valid_from`, `party_id` and `is_company` live on the
    Customer (`via: customer`), and so do a party's `role` and `account_id`;
  - TMF666 `BillingAccount` has no `validFor` and no characteristics (checked in
    the vendored bundle).
- **Local path, tests and goldens:** `idp run`, the test `lake` fixture and
  `make_golden.py` call `to_bronze` with canonical frames.
- **Gold and the MCP tools** don't use `party_id` or the accounts table, and
  nothing downstream reads `account_segment`. `account_tenure_days` is a model
  feature:
  - it's in `CREDIT_FEATURES` and `FRAUD_FEATURES` (`common/feature_contract.py`);
  - Gold copies it from `dim_subscriber` and divides by it for `arpu_monthly`;
  - the credit model's `affordability()` reads it;
  - the scoring API builds its frames from the contracts, defaulting a missing
    feature to 0;
  - trained models store their feature list, so a champion trained before a rename
    can't score renamed Gold.

## Goals / Non-Goals

**Goals:**
- The conformed records equal the generated canonical records exactly, the fact
  outputs (and their goldens) don't change, and Gold changes only by the renamed
  tenure column, with identical values. This is the regression guard for moving the
  adapter.
- Bronze never drops an event the consumer could store.
- No PII is copied into `silver.quarantine` (REQ-SLV-03).

**Non-Goals:**
- Iceberg VARIANT for `payload`: JSON text for now.
- Renaming `dim_subscriber.account_type`, which correctly is the billing account's
  type.
- Accepting the old feature name as an alias in the API: it's rejected with 422
  (§8).
- Automatic re-conforming of quarantined events: `--full-refresh` covers it.
- A streaming (always-on) Silver.

## Decisions

### 1. Raw Bronze tables
- **Table names.** One table per topic: `bronze.tmf_` + the topic suffix in snake
  case (`productUsage` → `tmf_product_usage`), from a map next to
  `TOPIC_RESOURCES`.
- **Columns:**

  | Column | Type | From |
  |---|---|---|
  | `tenant_id` | string | the topic |
  | `event_id`, `event_type` | string | the envelope |
  | `event_time` | string | the envelope, as received (parsed in Silver) |
  | `resource_type`, `resource_id` | string | the resource's `@type` and `id` |
  | `kafka_topic`, `kafka_key` | string | the message |
  | `kafka_partition`, `kafka_offset` | int, long | the message |
  | `payload` | string | the message value, verbatim |
  | `_ingested_at`, `ingest_date` | timestamp, date | landing |

- **Why JSON text?** It keeps the schema fixed whatever TMF or a customer profile
  adds (polymorphic characteristics would make a typed struct evolve constantly),
  and it's byte-exact. Iceberg VARIANT isn't usable from Spark 3.5 and pyiceberg
  yet.
- **One row builder.** `ingestion/raw.py: event_row(tenant, suffix, value_bytes,
  key, partition, offset)` builds the row. The consumer uses it per message.
  `idp run`, the test fixtures and the goldens use it through
  `raw_events(dataset, profile)`, which is the producer's `tenant_events` with
  synthetic offsets. So every path writes the same rows.
- **`to_bronze(cfg, rows_by_table)`** appends one snapshot per table, as now.

### 2. Consumer: land, don't interpret
- A message is dead-lettered only when `event_row` can't build a row:
  - the value isn't a JSON object;
  - `eventType` or `eventId` is missing;
  - `event` doesn't hold exactly one object.
- Schema, tenant, references and contract are Silver's business.
- `read_customers`, the customer index and the contract check leave the consumer.
- The `customer` topic stays compacted (unchanged, REQ-STR-01) and is consumed
  through the group like the others.
- Offsets, DLQ headers and the exit status are as before (REQ-STR-04).

### 3. Canonical model
The generator changes shape, not values (REQ-SYN-01). It makes no new random draws.

| Entity | Change |
|---|---|
| `parties` | drop `role` and `account_id` |
| `customers` (new) | `tenant_id`, `id` (`<tenant>-cust-<n>`, the existing `customer_id()` rule), `party_id`, `role` (the party's former role), `segment` (the account's former `account_segment`), `status` (`approved`), `valid_from` (the account's), `account_id` |
| `billing_accounts` (renamed from `accounts`) | `party_id` → `payer_party_id`; `account_segment` moves to the customer |
| `agreements` | add `customer_id` |

`ENTITIES` becomes `parties, customers, billing_accounts, agreements, products,
voice_events, data_events, recharge_events, fee_charges`. The Avro contracts follow:
`party`, new `customer`, `billing_account` (renamed from `account`) and
`agreement`.

### 4. Profile: Customer as a mapped entity, payer link
- **Customer** becomes a regular resource entry (`entity: customers`), and the
  special `customer:` section and `to_tmf`'s customer builder go. `_Customers`
  stays only as the adapter's index, used to resolve an agreement's `account_id`
  through its customer (`account[0].id`).
- **Agreement:**
  - `customer_id` is a path field: `engagedParty[0].partyOrPartyRole.id`, with
    `role: customer`, `@type: PartyRoleRef` and `@referredType: Customer`;
  - `account_id` keeps `via: customer`.
- **BillingAccount:**
  - `payer_party_id` is a path field: `relatedParty[0].partyOrPartyRole.id`, with
    `role: payer` and `@type: PartyRef`;
  - `is_company` maps `@referredType` (`true: Organization`, `false: Individual`);
  - `valid_from` (the account's opening date) becomes an extension attribute of
    `IdpBillingAccount` (`validFor.startDateTime`), declared in its extension
    schema. TMF666 has no `validFor`.
- **Customer:** `segment` is a `StringCharacteristic` named `segment`, as
  `accountSegment` was before. `valid_from` is `validFor.startDateTime`. Both are
  standard TMF629 fields.
- **Parties** need no reference resolution any more, so the `by_party` index goes.
- The producer's landing checks (dangling references, dependent files) keep working
  through the same adapter.

### 5. Silver conform step (`transforms/conform.py`)
- **Reference entities** (parties, customers, billing accounts, agreements,
  products), rebuilt every run like the dimensions:
  1. read the raw reference tables;
  2. pick the latest event per `(tenant_id, resource_id)` with a Spark window
     ordered by `to_timestamp(event_time)`, `_ingested_at`, `kafka_offset`
     (REQ-SLV-10);
  3. collect to the driver and conform in Python: per tenant, validate each event
     (`from_event`), index its latest customers, `adapt_resource`, and check the
     contract;
  4. build canonical Spark frames with the contracts' Spark schemas
     (`contracts.spark_schema`).

  The data is bounded by the subscriber count, like the dimension rebuild already
  is.
- **Facts** (usage, recharges, fee charges): conformed per micro-batch inside
  `foreachBatch` with `mapInPandas`. The output schema is the contract's columns
  plus `_entity`, `_quarantine_reason` and the event's metadata, and the frame is
  split on `_quarantine_reason`. No fact needs reference resolution (checked in the
  profile: usage carries `agreementId`, recharges the agreement extension, fees the
  product id). So the UDF is per resource and fully distributed. Validators and
  parsed contracts are compiled once per executor process (module-level caches).
- **Order.** The conformed records then go through the existing transforms
  (`build_fact_usage`, …). `fact_usage` comes from a single source table now, with
  voice and data from `usageType`.

### 6. Quarantine (`silver.quarantine`)
- **Columns:** `tenant_id`, `source_table`, `event_id`, `resource_type`,
  `resource_id`, `reason`, `event_time`, `quarantined_at`. Partitioned by
  `tenant_id`; merged insert-if-absent on `(tenant_id, source_table, event_id)`.
  Reference events are re-checked every run, so the merge is what keeps them from
  repeating (REQ-SLV-06).
- **No payload, no values in reasons.** Reasons name the field and the rule, never
  the offending value. For example, a contract failure says "`status`: not one of
  `RechargeStatus`", because fastavro's message would print the value, which could
  be an MSISDN. The event itself is found in Bronze by `event_id`.
- **Full refresh** drops the quarantine with the facts and rebuilds it (REQ-SLV-08),
  so fixed events leave it.
- Each run prints the quarantine count per source table. Quarantine doesn't fail
  the run (REQ-SLV-09).

### 7. Silver dimensions
- **`dim_customer`:** customers left-joined to parties for `party_type`. Columns:
  `customer_id`, `party_id`, `party_type`, `role`, `segment`, `status`,
  `valid_from` (UTC timestamp).
- **`dim_billing_account`:** `billing_account_id`, `account_type`,
  `lifecycle_status`, `valid_from`, `payer_party_id`, `is_company`.
- **`dim_product`:** `product_id`, `agreement_id`, `product_type`, `name`,
  `category`, `monthly_fee`, `lifecycle_status`.
- **`dim_subscriber`:** agreements → customers (on `customer_id`) → billing
  accounts (on `account_id`) → parties (on the customer's `party_id`), plus the
  product count. In `DIM_COLUMNS`:
  - `customer_id` replaces `party_id`;
  - `segment` (the customer's) replaces `account_segment`;
  - `customer_tenure_days` replaces `account_tenure_days`. It's computed in
    `build_dim_subscriber` with today's rule: whole elapsed days from the customer's
    `valid_from` to the latest customer `valid_from` in the run, floored, and 0 when
    unknown. It's stored in the table.

  In the generator the customer's party is the payer, and the customer's
  `valid_from` and segment are the account's former ones, so the values are
  unchanged.
- **Contracts:** the canonical contracts move to `config/avro/canonical/`, which is
  what the conform step enforces. `config/avro/silver/` documents every Silver
  table: `dim_customer`, `dim_billing_account`, `dim_product`, the updated
  `dim_subscriber`, and `fact_usage`, `fact_recharge`, `fact_fee_charge` and
  `quarantine`. Tests check each table's columns and timestamp types against them.
  (Decided after task 8.2: the facts had no contract, and the canonical files at the
  top level read like Bronze leftovers.)
- **PII:** no new PII columns (party ids aren't PII). `service_id` and `imsi` stay
  tokenized.

### 8. Gold and models (the tenure rename)
- **Feature contract:** `account_tenure_days` → `customer_tenure_days` in
  `CREDIT_FEATURES` and `FRAUD_FEATURES`, keeping its position, so the feature order
  and the Gold column order are otherwise unchanged.
- **Gold:** `build_credit_features` and `build_fraud_features` take it from
  `dim_subscriber`, and `arpu_monthly` divides by `customer_tenure_days`. The Gold
  tables change by that column name only.
- **Credit model:** `affordability()` reads `customer_tenure_days`. The PD model and
  the fraud model only see the renamed column. Their inputs are equal in value, so
  retraining on the same data and seed gives the same metrics; task 7.2 checks this
  against the earlier run.
- **Serving:** `publish` replaces the TimescaleDB tables (staging swap), so their
  columns follow Gold with no migration.
  - The API builds frames from the contracts, as now.
  - New: a `features` key that isn't in the endpoint's contract is a 422 naming it
    (REQ-SRV-02). It's checked before the model is loaded.
  - Missing keys still default to 0.
- **Registered models:** a champion trained before the change lists
  `account_tenure_days` and would fail on renamed Gold. After the reset its
  artifacts are gone from `idp-ml` too, while the MLflow registry (in the database)
  keeps its versions. So the migration retrains both models before Score runs.
  The gate is an absolute quality threshold, so the new versions become champion
  when they pass, as before.

### 9. Everything else
- **Maintenance** (`maintenance.py`) reads the new Bronze table names.
- **`idp migrate-lake` is removed**: `migration.py`, the CLI command, `make migrate-lake`,
  its Job and its tests, plus `catalog.append_chunks`, which only it used. It wrote
  canonical Bronze tables that Silver no longer reads (decided during task 8.2).
- **Required tables:** `REQUIRED_ENTITIES` becomes the raw `tmf_party`,
  `tmf_customer`, `tmf_billing_account` and `tmf_agreement` (REQ-SLV-04).
- **Decision record:** ADR-0032 records raw Bronze and conforming in Silver. It
  revises ADR-0031's "adapter in the consumer"; ADR-0031's wire contract stands.

## Risks / Trade-offs

- **[Bronze grows]** A raw event is about 2–3 KB of JSON, against about 0.3 KB for a
  canonical row. → Parquet (zstd) compresses repetitive JSON well. Bronze archiving
  (ADR-0028) already tiers old partitions. Measured on minikube (task 8.2): raw
  Bronze for 75,570 events (default volume, two tenants) is 6.5 MiB, of which
  `tmf_product_usage` is 5.9 MiB.
- **[Conforming cost moves to Silver]** Silver now runs the schema check, the
  adaptation and the contract check: about 10k resources a second per core. For
  75k events that's seconds over two executors. → Measured in task 8.2: `make silver`
  took 141 s end to end (pod start included) with an empty quarantine.
- **[Driver-side reference conform]** It's bounded by the subscriber count. At
  millions of subscribers it should become `applyInPandas` per tenant, which is
  noted as a follow-up.
- **["Latest" means the latest `eventTime`, which is the publish time]** Re-publishing
  an old landing file (`make stream-replay`) makes its versions the latest. → That's
  REQ-STR-01's definition of `eventTime`. A real source sets it to the change time.
  This is documented in the README.
- **[PII leaking into quarantine reasons]** → Reasons name fields and rules only
  (§6), and a test feeds a contract violation on a PII field and checks the reason.
- **[The goldens change]** → `dim_subscriber` (columns) and the new dimension
  goldens change. The fact goldens must stay byte-identical. The Gold goldens may
  differ only by the renamed column: the check compares the new frames with the old
  ones after renaming it. Task 7.3.
- **[API clients still send `account_tenure_days`]** → They get a 422 naming the
  field instead of a silent tenure of 0. The README example and the change log note
  the rename.
- **[A stale champion]** Until retraining, Score would fail on the renamed column,
  and so would the API's model path (the precomputed lookup still works). → The
  migration retrains before `make score`. A dry run of Score on old-name models
  fails loudly with a missing-column error, not wrong values.

## Migration Plan

1. You run `make minio-reset` (per the standing instruction), which empties Bronze,
   Silver, Gold and the catalog.
2. `make image`. Kafka topics and schemas are unchanged.
3. `make stream-from-landing`, then `make silver`, then `make gold`. Check that the
   quarantine is empty and that the new dimensions are present.
4. `make labels`, then `make train`. Both models get new versions, which pass the
   gate and become champion. Then `make score`, publish, and restart the scoring API
   (or wait for its champion refresh). Check one `POST /credit-limit` with
   `customer_tenure_days`, and that `account_tenure_days` is refused with 422.

**Rollback:** revert the code, run `make minio-reset` and stream again (the old
consumer rebuilds canonical Bronze from Kafka).

## Verification notes (task 8.2)

- **Model metrics.** The retrained champions (version 6) differ from version 5:
  credit KS 0.4412 → 0.3537, fraud PR-AUC 0.9659 → 0.9363. Version 5 was trained
  on an older lake (earlier runs' accumulated Bronze, before the committed code).
  Training the committed code and this change on the same generated data gives
  identical metrics to full precision (credit AUC 0.69850, KS 0.35367; fraud PR-AUC
  0.93630), equal to version 6. So the change itself leaves the metrics unchanged.
- **Customer events.** The first consume after the change landed 600
  `tmf_customer` events for 300 customers: the previous run's events were still on
  the compacted topic, and the old consumer never committed offsets there. Silver's
  latest-version rule picked the new ones; the quarantine stayed empty.

