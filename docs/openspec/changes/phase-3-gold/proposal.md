# Phase 3 — Gold Features

## Why
ML and serving need per-subscriber feature tables derived from Silver.

## What Changes
- Implement `features/base.py` (windowed recharge/usage aggregates),
  `features/credit_features.py`, `features/fraud_features.py`.
- Implement `pipeline.to_gold(cfg, silver=None, models=None)` — reads Silver from
  storage when not given; writes `credit_features`/`fraud_features` (+scores if
  models available) partitioned by `tenant_id`.

## Impact
- Spec deltas: `gold-features` (ADDED all requirements).
- Code: `src/idp/features/*`, `src/idp/pipeline.py` (`to_gold`).
- Decisions: ADR-0009 (standalone), ADR-0014 (features vs labels).

## Spec Deltas
Satisfies every requirement in `specs/gold-features`.
