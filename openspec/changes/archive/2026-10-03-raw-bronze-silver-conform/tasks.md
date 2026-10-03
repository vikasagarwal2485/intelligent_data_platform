# Tasks

## 1. Canonical model and contracts

- [x] 1.1 Reshape the generator (design §3):
  - `customers` (new, carrying `segment`, formerly the account's `account_segment`), `billing_accounts` (renamed from `accounts`, `party_id` → `payer_party_id`, no segment);
  - parties without `role` and `account_id`, and agreements with `customer_id`;
  - update `ENTITIES`, with no new random draws.

  Verify SCN-SYN-01.1 and 01.3 with `test_synthetic.py`, including a test that every value of the former model is carried unchanged: a dataset built at the previous commit, saved as a fixture, compared field by field after the renames and moves.
- [x] 1.2 Update `config/avro/`:
  - `party`, new `customer` (with `segment`), `billing_account` (replacing `account`, without segment) and `agreement`;
  - new `config/avro/silver/` contracts for `dim_customer` (with `segment`), `dim_billing_account` and `dim_product`, and `dim_subscriber` with `customer_id`, `segment` and `customer_tenure_days`.

  Add `contracts.spark_schema(name)` (Avro to Spark types: string, enum → string; double, long, boolean; nullable unions) and make contract error messages name the field and rule without the value. Verify with `test_contracts.py` (SCN-STR-03.3, plus a PII-in-reason test).

## 2. Mapping profile

- [x] 2.1 Make Customer a mapped entity in the profile engine (design §4):
  - drop the special `customer:` section and `to_tmf`'s customer builder, and keep `_Customers` as the adapter's index for an agreement's `account_id`;
  - drop the `by_party` resolution.

  Update `standard.yaml`:
  - the customers entry, with `segment` as a `StringCharacteristic` and `valid_from` as `validFor.startDateTime`;
  - the agreement's `customer_id`;
  - the BillingAccount's payer reference (`relatedParty` role `payer`, `is_company` from `@referredType`), with `validFor` as its only extension attribute (add it to `IdpBillingAccount.schema.json`).

  Verify:
  - SCN-BRZ-05.1 (byte-exact round trip), 05.2, 05.6, 05.7;
  - SCN-BRZ-06.1 (only standard, characteristic or extension fields);
  - the landing-check tests (dangling customer reference, dependent files), still passing.
- [x] 2.2 Regenerate `data_samples/` (add `customer.json` with its new shape and the payer-linked `billingAccount.json`) and verify `test_tmf_samples.py`.

## 3. Raw Bronze and the consumer

- [x] 3.1 Add `ingestion/raw.py`:
  - `event_row` and the topic → `bronze.tmf_*` table map;
  - `raw_events(dataset, profile)` (the producer's events with synthetic Kafka metadata);
  - change `pipeline.to_bronze` to append raw rows per table.

  Verify SCN-BRZ-01.1 to 01.5: the payload is byte-identical to the message, unmapped attributes are kept, invalid events are landed, partitioning, append-only.
- [x] 3.2 Rewrite the consumer to land raw rows (design §2): remove `read_customers`, the adaptation and the contract check, and dead-letter only messages `event_row` can't build. Update `idp consume`'s output. Verify SCN-STR-04.1 to 04.6 with the fake broker, plus offsets committed only after Bronze and the DLQ, and a failed landing committing nothing.

## 4. Silver conform and quarantine

- [x] 4.1 Add `transforms/conform.py`:
  - latest-version selection over the raw reference tables (REQ-SLV-10);
  - driver-side conforming of reference entities (validate, customer index, `adapt_resource`, contract) into Spark frames;
  - a `mapInPandas` conformer for fact batches, with the quarantine split.

  Verify:
  - SCN-SLV-09.1: conformed records equal the generated ones;
  - SCN-SLV-09.2 and 09.3: schema, foreign tenant, dangling customer and contract violation are quarantined;
  - SCN-SLV-10.1 and 10.2: the later version wins, landed in either order.
- [x] 4.2 Add `silver.quarantine` (merge insert-if-absent on `(tenant_id, source_table, event_id)`, dropped on full refresh) and per-source quarantine counts in `to_silver`'s output. Verify SCN-SLV-06 (no duplicate quarantine rows on re-run) and SCN-SLV-09.4 (fixed, then `--full-refresh` clears it).

## 5. Silver tables

- [x] 5.1 Build `dim_customer`, `dim_billing_account` and `dim_product`, and change `dim_subscriber` (design §7):
  - `customer_id` instead of `party_id`, joined through customers;
  - `segment` from the customer instead of `account_segment`;
  - `customer_tenure_days`, computed from the customer's `valid_from` and stored, instead of `account_tenure_days`.

   Point `build_dim` and `process_facts` at the conformed frames and the raw fact tables, and change `REQUIRED_ENTITIES` to the raw tables. Verify:
  - SCN-SLV-01.1 to 01.5 (including the tenure rule) and SCN-SLV-04.1;
  - SCN-STR-03.4 and 03.5 (columns equal the Silver contracts);
  - the existing `test_transforms_silver.py` and `test_silver_incremental.py` (SCN-SLV-07.x and 08.1), adapted to raw Bronze.

## 6. Gold and models

- [x] 6.1 Rename the feature `account_tenure_days` → `customer_tenure_days` (design §8):
  - in `CREDIT_FEATURES` and `FRAUD_FEATURES`, at the same position;
  - in `transforms/features.py` (`arpu_monthly`);
  - in `ml/credit_model.py` (`affordability`);
  - in the tests that build feature frames (`test_transforms_features.py`, `test_credit_model.py`, `test_curation.py`, the serving tests).

  Verify the feature, model and scoring tests, and that no `account_tenure_days` remains in `src/`.
- [x] 6.2 Make the scoring API reject unknown feature names with 422, checked against the endpoint's contract before the model is loaded; missing names still default to 0. Verify SCN-SRV-02.4 (`account_tenure_days` → 422 naming it) and SCN-SRV-02.5, plus the existing API tests.

## 7. Local path, fixtures and goldens

- [x] 7.1 Make `idp run`, the test `lake` fixture (`conftest.py`) and `make_golden.py` write raw Bronze through `raw_events`. Update `maintenance.py` and its tests for the new table names. Verify `test_maintenance.py` and `test_pipeline*`.
- [x] 7.2 Update the streaming tests end to end:
  - SCN-BRZ-03.4: generate, produce from landing, consume, then Silver conformed records equal the generated ones;
  - SCN-STR-01.x: unchanged, still passing.
- [x] 7.3 Regenerate the goldens. These may change:
  - `silver/dim_subscriber` (`customer_id`, `segment`, `customer_tenure_days`);
  - the new `silver/dim_customer`, `dim_billing_account` and `dim_product`;
  - `gold/*`, by the renamed tenure column only.

  Verify:
  - every `silver/fact_*` golden is byte-identical (`git diff`);
  - each Gold golden equals the committed one after renaming `account_tenure_days` → `customer_tenure_days` (a one-off script comparing the frames);
  - `dim_subscriber`'s `customer_tenure_days` and `segment` equal the committed `account_tenure_days` and `account_segment`;
  - the golden tests pass.

## 8. Docs and verification

- [x] 8.1 Update the docs:
  - `README.md`: the Bronze section (raw tables, columns, the `idp-bronze` walk-through), the Silver section (conform, quarantine, latest version, the new dimensions, the payer and customer links, `segment` and `customer_tenure_days`), the TMF mapping table (Customer as an entity, BillingAccount payer), the feature table, the scoring API example (`customer_tenure_days`, 422 for unknown names) and the testing notes;
  - `deploy/README.md`;
  - ADR-0032 in the decision log.

  Verify by reading.
- [x] 8.2 Run `pytest -q` until it's green. Then on minikube:
  - ask the user to run `make minio-reset`;
  - `make image`, `make stream-from-landing`, `make silver`, `make gold`;
  - check: Bronze holds the `tmf_*` tables with counts equal to the published events; `silver.quarantine` is empty; the new dimensions have one row per id and `dim_subscriber.customer_id` joins `dim_customer`; the fact counts equal the earlier run's (70,401 / 2,211 / 853 at the default volume); Gold builds with `customer_tenure_days`;
  - `make labels`, `make train`: both models get new versions that pass the gate and become champion. Compare their metrics with the last pre-change run's, which should be equal for the same data and seed;
  - `make score` and publish, then the serving API: `POST /credit-limit` with `customer_tenure_days` scores, and `account_tenure_days` is refused with 422;
  - record Bronze's size and Silver's run time.

  Keep the DAGs paused.
