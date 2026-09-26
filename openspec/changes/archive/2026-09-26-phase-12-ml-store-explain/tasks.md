# Phase 12 — Tasks

## Model store (REQ-ML-04)
- [x] `storage.model_uri/save_model/load_model/model_exists` (joblib+fsspec, local or s3://idp-ml)
- [x] `train` persists via the model store; `to_gold` loads from it; scoring API loads from it
- [x] `deploy/app/jobs/train-job.yaml` + `make train`
- [x] scoring-api Deployment gets `IDP_S3_*` env (loads models from MinIO)
- [x] Tests: local roundtrip; s3 model_uri; Gold loads models from store and emits scores

## Fee-charge features (REQ-ML-05, ADR-0023)
- [x] `fact_fee_charge` carries `agreement_id` (from the products join)
- [x] `features/base.fee_aggregates` attributes fees to a subscriber
- [x] Add fee features to `CREDIT_FEATURES`
- [x] Test: fee features present and attributed (>0)

## Reason codes (REQ-ML-01 / SCN-ML-03.2)
- [x] `CreditLimitModel.reason_codes` (SHAP when available, else scorecard)
- [x] `shap` as an optional extra in pyproject
- [x] `POST /credit-limit` returns `reason_codes`
- [x] Test: adverse-only, labelled with method

## Docs / spec
- [x] `specs/ml-models`: add REQ-ML-04, REQ-ML-05; update SCN-ML-03.2
- [x] `decisions/decision-log.md`: ADR-0023, ADR-0024
- [x] `roadmap.md` row 12; regenerate traceability
- [x] README ML section + bring-up ordering (make train before gold)
- [x] `make test` green
