# Tasks

## 1. Consent enforcement

- [ ] 1.1 Add `strict: bool = False` to `pii.filter_by_consent`: strict raises `ConsentError` naming the missing column, and null counts as false. Verify with unit tests for strict-missing, strict-null and non-strict no-op (existing test unchanged)
- [ ] 1.2 Filter `dim` with `filter_by_consent(dim, "analytics", strict=True)` at the start of `build_credit_features` and `build_fraud_features`. Verify with feature tests that a non-consenting subscriber is absent and consenting ones appear once (SCN-FND-05.1, 05.3)
- [ ] 1.3 Test that `to_gold` on a dim without `consent_analytics` raises and writes nothing (SCN-FND-05.4)
- [ ] 1.4 Test the end-to-end in-memory path: after `run(cfg, persist=False)`, no non-consenting token appears in training frames, `credit_scores`, `fraud_scores` or the SQLite-published serving tables (SCN-FND-05.2)
- [ ] 1.5 Test revocation: flip one subscriber's consent, rebuild Gold, score and publish, then verify that the subscriber is gone from Gold, the latest score run and serving (SCN-FND-05.5)
- [ ] 1.6 Update existing tests that assume feature rows == all subscribers. Verify that `pytest -q` is green
- [ ] 1.7 Fix the `src/idp/common/pii.py` module docstring so it matches the enforcement point

## 2. Spark engine

- [ ] 2.1 In `pipelines/spark/gold_job.py`, filter `dim` on `coalesce(consent_analytics, false)` and raise if the column is missing. Verify with the `spark-engine-parity` parity test (a pandas/Spark row match implies the same exclusion). If parity has not landed yet, add a standalone Spark test that asserts the exclusion

## 3. Tenant gateway in MCP

- [ ] 3.1 Replace the manual tenant filter in `mcp/tools.py::_read_layer_table` with `scope_to_tenant`. Verify with `test_mcp_tools.py` passing unchanged (SCN-FND-06.2)
- [ ] 3.2 Catch `TenantIsolationError` in `_lake_guard` and return `{"error": "tenant isolation violation…", "table": …}`. Verify with a test that writes an untagged Silver table and asserts the error and no rows (SCN-FND-06.1)
