# Phase 2 — Silver Curation

## Why
Downstream features/ML need clean, canonical, joinable, PII-safe tables rather
than raw JSON:API records.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- Implement `curation/jsonapi.py` (unwrap envelope, strip `UUID(...)`, normalize money) — `REQ-SLV-02`.
- Implement `curation/silver.py:curate()` producing `dim_subscriber`,
  `fact_usage`, `fact_recharge` (join, normalize, dedup) — `REQ-SLV-01`.
- Tokenize PII per tenant with consistent join tokens — `REQ-SLV-03`.
- Implement `pipeline.to_silver(cfg, bronze=None)` — reads Bronze from storage when
  not given; writes Silver partitioned by `tenant_id`; clear error on missing
  entities — `REQ-SLV-04`.

## Impact
- Spec deltas: `silver-curation` (ADDED all requirements).
- Code: `src/idp/curation/*`, `src/idp/pipeline.py` (`to_silver`, `_read_layer`).
- Decisions: ADR-0006 (PII), ADR-0009 (standalone stage).

## Spec Deltas
Satisfies every requirement in `specs/silver-curation`.
