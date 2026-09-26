# Proposal

## Why

REQ-ORC-02 promises that the Silver and Gold stages can switch engines
(`pandas` ↔ `spark`) without changing the DAG. In practice the Spark jobs produce a
different, smaller output:

- **Spark Silver** never builds `fact_fee_charge` and drops `agreement_type` from
  `dim_subscriber`.
- **Spark Gold** writes about 11 credit columns against the 25 `CREDIT_FEATURES` the
  model expects, and a fraction of the fraud set. It has no windowed features, no
  fee features and no derived ratios.

With `gold-engine=spark`, `idp score` and `idp train` would fail with a `KeyError`
on the model's feature columns. So the Spark path is not usable in production today.

## What Changes

- Spark Silver produces all four canonical tables with the same columns, types and
  tokens as the pandas Silver, including `fact_fee_charge` enriched from `products`.
- Spark Gold produces `credit_features` and `fraud_features` with exactly the pandas
  column set (keys, then `CREDIT_FEATURES` / `FRAUD_FEATURES` in order), the same
  lookback windows and the same derivations. Values match pandas within a stated
  tolerance.
- A parity check runs both engines on the same input and compares their outputs.
- Spark writes replace per-tenant partitions (dynamic partition overwrite), matching
  `idempotent-stage-writes`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `orchestration`: add REQ-ORC-04, pandas/Spark engine output parity.

## Impact

- Code: `pipelines/spark/silver_job.py` and `pipelines/spark/gold_job.py` (feature
  logic, and URI arguments so they can run on local paths); the spark ConfigMaps
  (`make spark-configmap`); the `SPARK_SALTS` wiring stays as-is.
- Tests: new parity tests, which need `pyspark` as an optional dev extra and are
  skipped when it is absent.
- Relationship to other changes:
  - Should land after `sync-specs-with-implementation`.
  - Pairs with `idempotent-stage-writes`, which gives the same overwrite semantics.
  - `consent-and-tenant-gateway` must apply its Gold consent filter in both engines.
