# Tasks

## 1. Consent enforcement (Spark feature transforms)

- [x] 1.1 Add `ConsentError` to `idp.common.pii` and `consenting_subscribers(dim, purpose="analytics")` to `idp.transforms.features` (keep rows where `coalesce(consent_<purpose>, false)` is true; raise `ConsentError` naming the column if it is missing). Verify with unit tests on hand-built DataFrames: true kept, false and null excluded, missing column raises (SCN-FND-05.3)
- [x] 1.2 Apply it to `dim` at the start of `build_credit_features` and `build_fraud_features`, and add `consent_analytics` to the `test_transforms_features.py` fixtures. Verify with tests that a non-consenting subscriber is absent from both tables and every consenting one appears exactly once (SCN-FND-05.1)
- [x] 1.3 Test that `to_gold` on a lake whose `dim_subscriber` has no `consent_analytics` column raises `ConsentError` and writes no Gold tables (SCN-FND-05.4)
- [x] 1.4 End-to-end on a local lake: after Silver, labels and Gold, then `train`, `to_scores` and `publish_serving` (SQLite engine), no non-consenting `service_id` appears in the training set, `credit_scores`, `fraud_scores` or any serving table (SCN-FND-05.2)
- [x] 1.5 Revocation: flip one consenting subscriber to `consent_analytics = false` in the Silver `dim_subscriber` (replace-mode rewrite), rerun Gold, Score and publish, and verify that the subscriber is gone from Gold, the latest score run and serving (SCN-FND-05.5)
- [x] 1.6 Regenerate `tests/golden/gold/` with `python tests/golden/make_golden.py`. Verify with a script that each new Gold table equals the previous one restricted to consenting `service_id`s (same columns, values within 1e-9), except the population-relative `recharge_amount_zscore`, which must equal a per-tenant z-score recomputed over the consenting subscribers only. `tests/golden/silver/` content must be unchanged. Also make the golden Parquet files tracked by git (`!tests/golden/**/*.parquet` after the `*.parquet` ignore rule; they were silently ignored before) and have `make_golden.py` skip Spark `.crc`/`_SUCCESS` files
- [x] 1.7 Update the tests that assume feature rows == all subscribers (`test_features.py`, `test_stages_lake.py`, and any others the suite reveals). Verify that `pytest -q` is green
- [x] 1.8 Fix the `src/idp/common/pii.py` module docstring so it names the enforcement point (`idp.transforms.features.consenting_subscribers`) and describes the fail-closed semantics

## 3. Tenant gateway in MCP

- [x] 3.1 Replace the manual tenant filter in `mcp/tools.py::_read_layer_table` with `scope_to_tenant`. Verify with `test_mcp_tools.py` passing unchanged (SCN-FND-06.2)
- [x] 3.2 Catch `TenantIsolationError` in `_lake_guard` and return `{"error": "tenant isolation violation…", "table": …}`. Verify with a test that writes an untagged Silver table and asserts the error and no rows (SCN-FND-06.1)

## 4. Cluster verification

- [x] 4.1 On minikube: `make image`, `make app-deploy`, then `make silver gold score publish`. Verify with an in-cluster check that the number of non-consenting `service_id`s (from Silver `dim_subscriber`) found in `gold.credit_scores`, `gold.fraud_scores` and the Gold feature tables is 0, and that the serving tables hold exactly the consenting subscribers
