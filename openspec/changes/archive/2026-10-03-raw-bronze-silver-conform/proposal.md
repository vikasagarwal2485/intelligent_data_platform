# Proposal: Raw Bronze, canonical Silver

## Why

In the medallion architecture, Bronze keeps the source data as received (immutable,
replayable, with ingestion metadata) and Silver conforms it to the enterprise model.
Since `tmf-streaming-to-silver`, this platform's consumer adapts the TM Forum events
to canonical records before Bronze. As a result:

- **Bronze loses source data.** Everything the mapping profile doesn't map is
  dropped before it is stored: `href`, unmapped characteristics, a customer's
  extension attributes, and the event metadata (`eventId`, `eventTime`, offsets).
- **A mapping bug can't be repaired by replay.** Bronze holds the adapter's output,
  so `idp silver --full-refresh` re-reads the wrong values.
- **Valid source data is turned away at ingestion.** An event that fails the
  canonical contract, or whose customer isn't known yet, goes to the DLQ instead of
  Bronze.
- **The consumer does Silver's work.** It validates, resolves Customer references
  (reading the compacted topic in full every run) and adapts.
- **Silver keeps an arbitrary version of a party** when Bronze holds several, and
  has no event time to choose by.

At the same time, the canonical model doesn't separate the billing account's payer
from the customer relationship. Silver also has no tables of its own for billing
accounts, products or customers.

## What Changes

- **Bronze becomes raw TMF.** **BREAKING** for anything reading Bronze.
  - There is one table per topic: `bronze.tmf_customer`, `tmf_party`,
    `tmf_billing_account`, `tmf_agreement`, `tmf_product`,
    `tmf_applied_customer_billing_rate`, `tmf_topup_balance`, `tmf_product_usage`.
  - Each row is one event as received (`payload`, the JSON string), plus envelope
    and Kafka metadata as columns.
  - Bronze stays append-only and partitioned by `(tenant_id, ingest_date)`.
  - The canonical tables `bronze.parties`, `bronze.accounts` and so on go away.
- **The consumer only lands raw events.** It no longer validates, resolves or
  adapts. Only a message it can't store (not JSON, not a TMF event) goes to the DLQ.
- **Silver conforms.** A new conform step:
  - parses the raw events with the TMF mapping profile (REQ-BRZ-05);
  - resolves references between the tenant's latest resources;
  - checks the schemas, the `tenantId` and the canonical Avro contract;
  - keeps the **latest version** of each reference resource, by event time.

  A record that fails goes to a new `silver.quarantine` table with the reason, and
  Bronze still holds it for reprocessing. The existing Silver transforms then run
  on the conformed records.
- **The canonical model changes:**
  - `accounts` is renamed `billing_accounts`, and its `party_id` becomes
    `payer_party_id`;
  - a TMF `BillingAccount` references its paying party directly (`relatedParty`
    role `payer`, Individual or Organization);
  - `customers` becomes a canonical entity: the TMF629 Customer (a party role
    `customer`) with its party, `segment`, status, since-date and account. The
    segment moves here from the billing account (`account_segment`, renamed
    `segment`), because it describes the customer;
  - agreements carry `customer_id`;
  - parties lose `role` and `account_id`, which belong to the customer.
- **New Silver tables:** `dim_billing_account`, `dim_product` and `dim_customer`.
  In `dim_subscriber`:
  - `customer_id` replaces `party_id`;
  - `segment` (the customer's) replaces `account_segment`;
  - `customer_tenure_days`, computed from the customer's since-date and stored,
    replaces `account_tenure_days`.
- **Gold and models follow the tenure rename.** **BREAKING** for scoring API
  clients and for registered models.
  - The feature `account_tenure_days` becomes `customer_tenure_days` in the credit
    and fraud feature contracts, the Gold feature tables (and `arpu_monthly`, which
    divides by it), the credit model's affordability formula, the serving tables
    and the scoring API's feature names.
  - The values don't change: a customer's since-date equals its billing account's.
    So retrained models should reach the same metrics.
  - Models registered before the change list the old feature name, so both models
    are retrained after migration.
  - The scoring API rejects feature names outside the model's contract with HTTP 422
    instead of silently scoring them as `0`. A client still sending
    `account_tenure_days` gets an error, not a tenure of zero.
- **Migration:** the user runs `make minio-reset`; Bronze and Silver are rebuilt
  by a fresh stream.
- **`idp migrate-lake` is removed** (with its Makefile target, Job and tests). It
  converted a legacy Parquet Bronze into canonical Bronze tables, which Silver no
  longer reads; a legacy lake is reset and re-streamed instead.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `bronze-ingestion`:
  - REQ-BRZ-01: Bronze holds raw TMF events with ingestion metadata;
  - REQ-BRZ-03: the landing feeds raw Bronze (scenario 03.4);
  - REQ-BRZ-05: the adapter runs in Silver's conform step, yields `customers`,
    resolves the payer and customer links, and keeps the latest version. (The
    billing account's validity becomes a platform extension attribute, which
    REQ-BRZ-06 already allows, because TMF666 has no field for it. The segment moves
    to the Customer, as a standard characteristic.)
- `streaming-kafka`:
  - REQ-STR-02 is removed and replaced by new REQ-STR-04: the consumer lands raw
    events, and dead-letters only messages it can't store (removed rather than
    modified, because the old scenarios' titles say the opposite);
  - REQ-STR-03: the canonical contract is enforced at Silver conform, and covers
    the new entities and Silver tables.
- `silver-curation`:
  - REQ-SLV-01: the Silver tables, including the three new dimensions and
    `dim_subscriber`'s `customer_id`;
  - REQ-SLV-02: Silver now parses source resources;
  - REQ-SLV-04: the raw Bronze tables it requires;
  - REQ-SLV-07: incremental facts from the raw event tables;
  - new REQ-SLV-09: conform and quarantine;
  - new REQ-SLV-10: latest version wins.
- `synthetic-data`:
  - REQ-SYN-01: the generator emits `customers` (with the segment) and
    `billing_accounts` (with the payer party), agreements with `customer_id`, and
    parties without role and account.
- `serving-api`:
  - REQ-SRV-02: unknown feature names are rejected with HTTP 422.
- `gold-features` and `ml-models` need no delta: their requirements name no
  feature columns. The rename is covered by the design and tasks.
- `platform-deployment`:
  - REQ-PLT-11: the read-only scenario names a raw Bronze table.

## Impact

- **Code:**
  - `streaming/consumer.py`: raw landing only;
  - `pipeline.py`: `to_bronze` writes raw events; `idp run` goes through TMF;
  - `transforms/`: a new conform module (`mapInPandas` over the profile adapter),
    and changes to `incremental.py` and `silver.py` (new dimensions,
    `customer_id`, latest version, quarantine);
  - `ingestion/tmf/profile.py`: Customer becomes a mapped entity, plus the payer
    link;
  - `config/tmf/profiles/standard.yaml`;
  - the `IdpBillingAccount` extension schema;
  - `synthetic/generator.py`;
  - `common/contracts.py` and `config/avro/`: the canonical contracts move to
    `canonical/` (with the new and renamed ones), and `silver/` gets a contract for
    every Silver table (dimensions, facts, quarantine);
  - `maintenance.py` (table names);
  - `common/feature_contract.py`, `transforms/features.py`, `ml/credit_model.py` and
    `serving/api.py` (the tenure rename and the 422).
- **Tests:**
  - the `lake` fixture, `idp run`, `make_golden.py` and the streaming, Silver,
    TMF and contract tests;
  - the Silver golden data for `dim_subscriber` is regenerated, and the new
    dimensions get golden files;
  - the fact golden data must not change, and the Gold golden data may change only
    by the renamed column, with identical values;
  - feature, credit-model and API tests use the new name.
- **Models:** both are retrained after migration (`make labels`, `make train`),
  and the new versions become champion through the existing gate. Score and the
  API use them after that.
- **Kafka:** topics, keys and events are unchanged. The `customer` topic stays
  compacted, but the consumer no longer reads it in full.
- **Docs:** README (Bronze and Silver sections, the TMF mapping, the feature table
  and the API example), `deploy/README.md`,
  and ADR-0032 (raw Bronze), which revises ADR-0031's "adapter in the consumer".
- **Out of scope:**
  - renaming `dim_subscriber.account_type`, which is correctly the billing
    account's type;
  - Iceberg VARIANT for `payload`;
  - replaying quarantined records automatically (a full refresh does it).
