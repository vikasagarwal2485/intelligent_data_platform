# Phase 12 — Shared model store, fee-charge features, adverse-action reason codes

Date: 2026-07-20

## Why
Three ML gaps surfaced once the platform ran in-cluster:
1. **Models never reached the Gold/serving pods.** `train`, `gold` and `scoring-api`
   are separate pods; models were saved to an ephemeral per-pod `ml/artifacts`
   dir with no shared/persistent store. So `to_gold` found no model → produced
   feature tables only → `publish` wrote `gold.credit_features`/`fraud_features`
   with **no scores**. There was also no in-cluster train job at all.
2. **Fee-charge data was analytics-only** (ADR-0022) — a real spend/commitment
   signal was not available to the credit model.
3. **No adverse-action reason codes** (SCN-ML-03.2) — only the scorecard existed
   as an interpretability path; nothing produced per-decision codes.

## What Changes
Each item cites the Requirement id(s) it satisfies.

- **Shared model store (MinIO `s3://idp-ml`)** — `common/storage.py` gains
  `model_uri` / `save_model` / `load_model` / `model_exists` (joblib over fsspec,
  local dir or S3 by URI scheme). `train` writes there; `to_gold` and the scoring
  API load from there — `REQ-ML-04`.
- **`train-job.yaml` + `make train`** — an on-demand in-cluster training Job that
  writes models to `s3://idp-ml`; scoring API Deployment gains the `IDP_S3_*` env
  so it loads models from the same store — `REQ-ML-04`, `REQ-ML-03`.
- **Fee-charge features fed to the credit model** — Silver `fact_fee_charge` now
  carries `agreement_id` (from the product join); `features/base.fee_aggregates`
  attributes fees to a subscriber (`agreement_id → service_id`) and adds
  `fee_total`, `fee_recurring_total`, `fee_overage_count`, `fee_charge_count`,
  `fee_total_30d` to `CREDIT_FEATURES` — `REQ-ML-05` (reverses ADR-0022 for the
  credit model; ADR-0023).
- **Adverse-action reason codes** — `CreditLimitModel.reason_codes()` returns the
  top-k features raising PD, via **SHAP** (`TreeExplainer`) when the optional
  `shap` package is present, else the interpretable **logistic-scorecard**
  contributions. Exposed in the `POST /credit-limit` response as `reason_codes` —
  `REQ-ML-01`, `SCN-ML-03.2`.

## Impact
- Spec deltas: `ml-models` — ADD `REQ-ML-04`, `REQ-ML-05`; UPDATE `SCN-ML-03.2`
  (reason codes now implemented).
- Code: `common/storage.py`, `pipeline.py`, `serving/api.py`, `ml/credit_model.py`,
  `features/base.py`, `features/credit_features.py`, `curation/silver.py`,
  `deploy/app/deployment/scoring-api.yaml`, `deploy/app/jobs/train-job.yaml`,
  `Makefile`, `pyproject.toml`, `tests/**`.
- Decisions: ADR-0023 (fee-charge features for credit), ADR-0024 (MinIO model
  store + reason codes).

## Bring-up order (local)
`… make stream → make silver → make train → make gold → make publish`. `make
train` is new and must run before `make gold` for scores to appear; without it,
Gold still produces the feature tables (graceful degrade).

## Spec Deltas
Satisfies `REQ-ML-04`, `REQ-ML-05` and the `SCN-ML-03.2` scenario in
`specs/ml-models`.
