# Phase 3 — Gold Features

## Why
ML and serving need per-subscriber feature tables derived from Silver.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- Implement `features/base.py` (windowed aggregates), `features/credit_features.py`,
  `features/fraud_features.py` — `REQ-GLD-01`, `REQ-GLD-02` (tokenized join).
- Implement `pipeline.to_gold(cfg, silver=None, models=None)` — reads Silver from
  storage when not given; writes `credit_features`/`fraud_features` (+scores only
  if models available) partitioned by `tenant_id` — `REQ-GLD-01`, `REQ-GLD-03`.

## Impact
- Spec deltas: `gold-features` (ADDED all requirements).
- Code: `src/idp/features/*`, `src/idp/pipeline.py` (`to_gold`).
- Decisions: ADR-0009 (standalone), ADR-0014 (features vs labels).

## Spec Deltas
Satisfies every requirement in `specs/gold-features`.
