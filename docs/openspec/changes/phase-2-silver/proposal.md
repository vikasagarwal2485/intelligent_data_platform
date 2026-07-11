# Phase 2 — Silver Curation

## Why
Downstream features/ML need clean, canonical, joinable, PII-safe tables rather
than raw JSON:API records.

## What Changes
- Implement `curation/jsonapi.py` (unwrap envelope, strip `UUID(...)`, normalize money).
- Implement `curation/silver.py:curate()` producing `dim_subscriber`,
  `fact_usage`, `fact_recharge` (join, normalize, tokenize PII, dedup).
- Implement `pipeline.to_silver(cfg, bronze=None)` — reads Bronze from storage when
  not given; writes Silver partitioned by `tenant_id`; clear error on missing entities.

## Impact
- Spec deltas: `silver-curation` (ADDED all requirements).
- Code: `src/idp/curation/*`, `src/idp/pipeline.py` (`to_silver`, `_read_layer`).
- Decisions: ADR-0006 (PII), ADR-0009 (standalone stage).

## Spec Deltas
Satisfies every requirement in `specs/silver-curation`.
