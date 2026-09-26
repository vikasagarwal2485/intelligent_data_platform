# Phase 4 — Credit-Limit ML

## Why
Deliver the alternative credit-limit product: probability-of-default scoring and
affordability-based limit sizing.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- Implement `ml/credit_model.py`: `heuristic_limit`, `CreditLimitModel` (PD
  gradient-boosted classifier + logistic scorecard baseline, `predict_pd`,
  `recommend_limit`, `evaluate` with AUC/KS/Gini, save/load) — `REQ-ML-01`.
- Wire the credit half of `pipeline.train` (features + labels → out-of-sample fit,
  persist model) — `REQ-ML-03`.

## Impact
- Spec deltas: `ml-models` (ADDED credit + training requirements).
- Code: `src/idp/ml/credit_model.py`, `src/idp/pipeline.py` (`train`, `_safe_split`,
  `_tokenize_labels`).
- Decisions: ADR-0010 (sklearn stand-in), ADR-0014 (labels offline), ADR-0015.

## Spec Deltas
Satisfies the credit-limit and training requirements in `specs/ml-models`.
