# Tasks

## 1. Storage write modes

- [ ] 1.1 Add `mode: Literal["replace", "append"] = "replace"` to `storage.write_parquet`. For partitioned writes, map `replace` to `existing_data_behavior="delete_matching"` and `append` to `"overwrite_or_ignore"`. Verify with a unit test that three `replace` writes leave 1× rows and three `append` writes leave 3× rows
- [ ] 1.2 Unit-test per-tenant replace: write tenant_a and tenant_b, then replace with tenant_a-only data. Verify that tenant_b's rows are unchanged (SCN-SLV-06.2)

## 2. Stage callers

- [ ] 2.1 `to_silver`, `to_gold` and `to_labels` use the default replace mode. Verify with tests that run each stage twice and assert unchanged row counts (SCN-SLV-06.1, SCN-GLD-04.1, SCN-ML-08.1)
- [ ] 2.2 `to_scores` passes `mode="append"`. Verify with a test that two runs yield two `scored_at` values, each with one row per subscriber (SCN-ML-08.2)
- [ ] 2.3 Test that a Gold refresh after changed Silver leaves no stale rows (SCN-GLD-04.2)

## 3. Publish latest and uniqueness

- [ ] 3.1 In `publish_serving`, reduce `credit_scores` and `fraud_scores` to each tenant's latest `scored_at` before publishing. Verify with a test using the SQLite engine and two runs (SCN-SRV-05.1)
- [ ] 3.2 Make the `(tenant_id, service_id)` index UNIQUE in `timescale.publish_frame`. Verify with a test that a duplicate-key frame raises and the prior table's contents survive (SCN-SRV-05.2)
- [ ] 3.3 Test that `read_score` returns the latest-run row after publishing two runs

## 4. Verification

- [ ] 4.1 `pytest -q` is green, including the existing pipeline and serving tests
- [ ] 4.2 On minikube, run `make silver gold labels` twice, then `make score` twice and `make publish`. Verify with `make timescale-verify` and a MinIO row-count check that there are no duplicates and the serving tables hold one row per key
- [ ] 4.3 Document the one-off migration (clean rebuild) in `deploy/README.md`
