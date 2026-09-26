# Design

## Context

- `dim_subscriber` carries `consent_analytics` and `consent_marketing` from `parties`
  in both engines.
- `pii.filter_by_consent` returns its input unchanged when the column is missing, and
  a test asserts that no-op.
- The feature builders (`build_credit_features`/`build_fraud_features`) start from
  `silver["dim_subscriber"]`. Both the lake path (`to_gold`) and the in-memory
  training path (`train(dataset=…)`) go through them.
- In MCP, `_read_layer_table` filters `df[df.tenant_id == t]`, and returns an empty
  frame when `tenant_id` is missing.

## Goals / Non-Goals

**Goals:**
- One enforcement point for consent that every ML consumer inherits.
- Fail closed on null or missing consent.
- The MCP read path uses the tenant gateway; untagged data is an error.

**Non-Goals:**
- **Aggregate Silver analytics (MCP tools 1–5)** keep counting every subscriber.
  They return tenant-level aggregates, not per-subscriber profiling, and are
  operational reporting (subscriber counts, revenue). If the DPO rules otherwise,
  apply the same filter in those tools later.
- **On-demand API scoring** (`/credit-limit`, `/fraud-score`). The caller supplies
  the features, and the API has no subscriber consent context. Precomputed lookups
  are covered, because non-consenting subscribers are never published.
- Marketing consent (`consent_marketing`), which nothing currently uses.
- Deleting historical score runs for revoked subscribers. They age out of the latest
  run immediately; purging history is part of a future retention change.

## Decisions

- **Filter in the feature builders, not in `to_gold`/`train`/`to_scores`
  separately.** A shared helper filters `dim` through
  `pii.filter_by_consent(dim, "analytics", strict=True)` before the aggregates are
  merged. Every path that builds features (the lake Gold stage, the in-memory train
  path, `idp run`, tests) inherits it.
  - Alternative: filter at Score and train time. Rejected because it needs the
    consent column carried into Gold and adds three enforcement points.
- **`strict` parameter on `filter_by_consent`.**
  - `strict=True` raises `ConsentError` when the column is absent, and treats null as
    false (it already does `fillna(False)`).
  - `strict=False` keeps today's no-op for backwards compatibility, and the existing
    test stays valid.
- **Spark Gold applies the equivalent filter** (`coalesce(consent_analytics, false)`)
  on `dim` and raises if the column is missing. The parity test from
  `spark-engine-parity` then covers it automatically.
- **Gateway in MCP.** `_read_layer_table` calls `scope_to_tenant(df, tenant_id)` in
  place of the manual filter. `TenantIsolationError` is caught in `_lake_guard` and
  returned as `{"error": "tenant isolation violation: …", "table": …}`.
  `FileNotFoundError` still means "not written yet", so empty stays empty.

## Risks / Trade-offs

- [Shrinking the training set (~10%) shifts model metrics slightly] → Expected; the
  synthetic labels are independent of consent. Keep the existing metric thresholds
  in tests.
- [Tests asserting feature rows == subscribers] → Update them to count consenting
  subscribers.
- [Strict mode breaks old lakes whose `dim_subscriber` lacks the consent column] →
  Both engines have always emitted it. Rebuilding Silver fixes any exception.
- [Revocation is only as fresh as the Gold/Score cadence (hourly features, then
  dataset-driven inference)] → Document it as the SLA.

## Migration Plan

1. Merge after `idempotent-stage-writes`, so refreshes remove old rows.
2. Run `idp gold`. Then `idp score` and `idp publish` (or let `idp_inference` fire
   on the gold-features dataset).
3. Verify that the count of non-consenting service_ids in `gold.credit_scores` is 0.
4. Rollback: revert the image. The next Gold refresh re-includes everyone.
