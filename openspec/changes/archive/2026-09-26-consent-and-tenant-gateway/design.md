# Design

## Context

- Silver `dim_subscriber` carries `consent_analytics` and `consent_marketing` from
  `parties` (`idp.transforms.silver`).
- The Gold feature tables are built by Spark in `idp.transforms.features`.
  `build_credit_features`/`build_fraud_features` start from
  `silver["dim_subscriber"]`, a Spark DataFrame read from the lake. Since
  `spark-single-engine`, every path that builds features goes through them:
  - `to_gold`, which backs `idp gold`, the Airflow task, `idp run` and the tests
  - the `spark-submit` wrapper

  There is no in-memory feature path any more.
- `pii.filter_by_consent` is a pandas row filter that returns its input unchanged
  when the column is missing (a test asserts that). It is not on the feature path.
- `tests/golden/gold/` holds reference Gold tables that include every subscriber.
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

- **Filter once, in the Spark feature builders.** A helper
  `consenting_subscribers(dim, purpose="analytics")` in `idp.transforms.features`:
  - keeps the rows where `coalesce(consent_<purpose>, false)` is true (null or false
    means excluded: fail closed)
  - raises `pii.ConsentError` naming the column when it is missing

  `build_credit_features` and `build_fraud_features` apply it to `dim` before any
  aggregate is joined. Every consumer of Gold inherits it: training, Score, publish
  and the score analytics.
  - Alternative: filter at Score and train time. Rejected because it needs the
    consent column carried into Gold and adds several enforcement points.
- **`ConsentError` lives in `idp.common.pii`**, next to the consent concept and
  importable without Spark. The pandas `filter_by_consent` stays as it is: REQ-FND-03.2
  still describes it, and it isn't on the feature path.
- **The golden Gold files are regenerated with `tests/golden/make_golden.py`** once
  the filter is in. A check proves the new tables equal the old ones restricted to
  consenting subscribers, so the filter is the only behavioural change. The Silver
  goldens stay byte-for-byte unchanged.
- **Gateway in MCP.** `_read_layer_table` calls `scope_to_tenant(df, tenant_id)` in
  place of the manual filter. `TenantIsolationError` is caught in `_lake_guard` and
  returned as `{"error": "tenant isolation violation: …", "table": …}`.
  `FileNotFoundError` still means "not written yet", so empty stays empty.

## Risks / Trade-offs

- [Shrinking the training set (~10%) shifts model metrics slightly] → Expected; the
  synthetic labels are independent of consent. Keep the existing metric thresholds
  in tests.
- [Tests asserting feature rows == subscribers, and the hand-built `dim` fixtures in
  `test_transforms_features.py` that have no consent column] → Count consenting
  subscribers instead, and add `consent_analytics` to the fixtures.
- [Regenerating golden files could hide an unintended change] → The regeneration
  task diffs old against new and allows only the removal of non-consenting rows.
- [Fail-closed breaks old lakes whose `dim_subscriber` lacks the consent column] →
  Silver has always emitted it. Rebuilding Silver fixes any exception.
- [Revocation is only as fresh as the Gold/Score cadence (hourly features, then
  dataset-driven inference)] → Document it as the SLA.

## Migration Plan

1. Merge after `idempotent-stage-writes`, so refreshes remove old rows.
2. Run `idp gold`. Then `idp score` and `idp publish` (or let `idp_inference` fire
   on the gold-features dataset).
3. Verify that the count of non-consenting service_ids in `gold.credit_scores` is 0.
4. Rollback: revert the image. The next Gold refresh re-includes everyone.
