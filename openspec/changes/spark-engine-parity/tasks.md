# Tasks

## 1. Test harness

- [ ] 1.1 Add a `spark = ["pyspark==3.5.3"]` optional extra in `pyproject.toml`, register the `spark` pytest marker, and add a module-scoped local SparkSession fixture (UTC, `local[2]`). Verify that `pytest -m spark` collects, and skips cleanly without pyspark
- [ ] 1.2 Add `--bronze/--silver/--gold` URI arguments to both jobs (defaulting to `s3a://<prefix>-<layer>`), and set `partitionOverwriteMode=dynamic` and `session.timeZone=UTC` in each SparkSession. Verify that the jobs run against local temp dirs from the test fixture

## 2. Spark Silver parity

- [ ] 2.1 Keep `agreement_type` in Spark `dim_subscriber`, and align column order and types with pandas `build_subscriber_dim`. Verify with a parity test on `dim_subscriber`
- [ ] 2.2 Build `fact_fee_charge` in Spark: strip UUIDs, UTC `charged_at`, fill null `amount` with 0, de-duplicate on `(tenant_id, id)`, and left-join `products` for `product_name`/`category`/`agreement_id`. Verify with a parity test on `fact_fee_charge` (SCN-ORC-04.1)
- [ ] 2.3 Parity-test `fact_usage` and `fact_recharge`, including identical tokens. Verify that SCN-ORC-04.1 passes for all four tables

## 3. Spark Gold parity

- [ ] 3.1 Port the recharge aggregates: counts, success rate, avg/max/total, `days_since_last_recharge`, `share_credit_card`, and windowed totals/counts for the configured windows. Verify with a parity test on these columns
- [ ] 3.2 Port the usage aggregates: charged amount, voice minutes/calls, data bytes, roaming count/share, `originating_ratio`, unique called numbers, `calls_per_active_day`, and windowed amounts/bytes. Verify with a parity test on these columns
- [ ] 3.3 Port the fee aggregates via the `agreement_id` → `service_id` attribution, plus the derived `arpu_monthly`/`spend_to_recharge_ratio` and int casts. Verify with a full `credit_features` parity test (SCN-ORC-04.2)
- [ ] 3.4 Port the fraud set, including the per-tenant `recharge_amount_zscore`. Verify with a full `fraud_features` parity test (SCN-ORC-04.2)
- [ ] 3.5 Assert that the literal column lists in `gold_job.py` equal `CREDIT_FEATURES`/`FRAUD_FEATURES`, so drift fails the test

## 4. End-to-end

- [ ] 4.1 Test: train on pandas features, then score the Spark-built Gold. Verify that `credit_scores`/`fraud_scores` are produced (SCN-ORC-04.3)
- [ ] 4.2 Test: Spark writes tenant_a only, and the tenant_b partition is unchanged (SCN-ORC-04.4)
- [ ] 4.3 On minikube, run `make spark-configmap`, set `gold-engine ENGINE=spark` and `silver-engine ENGINE=spark`, then trigger `idp_features`. Verify that `idp_inference` succeeds and `make timescale-verify` shows the score tables
