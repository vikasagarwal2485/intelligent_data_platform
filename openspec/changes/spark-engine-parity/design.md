# Design

## Context

- `pipelines/spark/{silver,gold}_job.py` are standalone scripts mounted from the
  ConfigMaps. They run in `apache/spark:3.5.3`, which cannot import the `idp`
  package.
- They hard-code `s3a://<prefix>-<layer>` paths.
- The pandas logic lives in `src/idp/curation/silver.py` and
  `src/idp/features/{base,credit_features,fraud_features}.py`.
- The Spark Silver job already mirrors tokenization (`tok_` + sha256[:24] with the
  same salt fallback) and `UUID(...)` stripping. The gaps are:
  - Silver: `fact_fee_charge` is missing, and `agreement_type` is dropped from `dim`.
  - Gold: most feature columns are missing (see proposal).

## Goals / Non-Goals

**Goals:**
- Column- and value-level parity for both stages, proven by an automated test.
- Scripts runnable against local paths so the test needs no MinIO.

**Non-Goals:**
- Sharing code between the engines by packaging `idp` into the Spark image. Worth
  revisiting later; for now the scripts stay self-contained.
- Iceberg/Nessie reads and writes.
- Spark cluster mode or the Spark Operator. `local[*]` in a pod stays.

## Decisions

- **Feature column lists are copied as literal constants into `gold_job.py`**,
  because the script cannot import `idp`. The parity test asserts that they equal
  `CREDIT_FEATURES`/`FRAUD_FEATURES`, so drift fails CI.
  - Alternative: generate the ConfigMap from a template with the lists injected.
    Rejected as extra build machinery.
- **Reference-time semantics mirror pandas exactly.** Each aggregate family uses its
  own table's max timestamp as the window reference: recharge →
  `max(created_at)`, usage → `max(started_at)`, fees → `max(charged_at)`. Windows are
  `>= ref - N days`. Missing aggregates become `0.0` after the left joins, as
  `merge_features` does.
- **Derived features** port as formulas:
  - `arpu_monthly = usage_charged_amount / (max(account_tenure_days, 30) / 30)`,
    rounded to 4 places
  - `spend_to_recharge_ratio`: 0 when `recharge_total` is 0, rounded to 4 places
  - `calls_per_active_day = voice_call_count / distinct call dates`
  - `recharge_amount_zscore`: per-tenant population z-score (`stddev_pop`), 0 when
    the standard deviation is 0 or null
  - booleans are cast to int
- **Fee attribution in Spark:** `fee_charges` → strip UUIDs → join `products` for
  `product_name`/`category`/`agreement_id` → Silver. In Gold, join `dim` on
  `(tenant_id, agreement_id)` to reach `service_id`, as `fee_aggregates` does.
- **Path arguments.** Both jobs gain `--bronze/--silver/--gold` URI options that
  default to `s3a://<prefix>-<layer>`, so the DAG and Job manifests stay unchanged
  and tests pass `file://` temp dirs.
- **Dynamic partition overwrite.** Set
  `spark.sql.sources.partitionOverwriteMode=dynamic` in the job's SparkSession.
  Writes stay `mode("overwrite").partitionBy("tenant_id")`, and that now replaces
  only the tenants present (SCN-ORC-04.4).
- **Parity test** (`tests/test_spark_parity.py`):
  1. generate a small synthetic dataset
  2. write Bronze locally
  3. run pandas `curate` and Spark `build_silver` into two dirs
  4. compare with `pandas.testing.assert_frame_equal` after sorting by keys, using
     `check_dtype=False` and `atol=1e-6`
  5. run the same comparison for Gold
  6. score the Spark-built Gold with a freshly trained model

  It is marked `spark` and skipped unless `pyspark` is importable. `pyspark` becomes
  a `[spark]` optional extra.

## Risks / Trade-offs

- [Floating-point summation order differs between engines] → An absolute tolerance
  of 1e-6 and comparison after rounding the rounded columns. If a column is
  inherently unstable, the test documents a looser per-column tolerance.
- [Timestamp timezone handling: Spark session TZ vs pandas UTC] → Set
  `spark.sql.session.timeZone=UTC` in both jobs.
- [Categorical `tenant_id` from partition discovery] → Cast to string on read in the
  test.
- [Test runtime: a local Spark start takes ~10–20 s] → One module-scoped
  SparkSession fixture; small dataset (~50 subscribers/tenant, 14 days).
- [Duplicated logic can drift] → The parity test is the guard; run it in CI with the
  `[spark]` extra.

## Migration Plan

1. Merge; `make spark-configmap` refreshes the scripts.
2. `make gold-engine ENGINE=spark`, trigger `idp_features`, then confirm that
   `idp_inference` scores successfully (SCN-ORC-04.3).
3. Rollback: `make gold-engine ENGINE=pandas` (no data migration, since the schemas
   are identical).
