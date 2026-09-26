# Phase 1 — Bronze Ingestion

## Why
The lakehouse needs an immutable, tenant-partitioned raw layer as the source of
truth for all downstream curation.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- Implement `ingestion/bronze.py` (persist/load raw frames, tenant-partitioned) — `REQ-BRZ-01`.
- Implement `pipeline.to_bronze(cfg, frames)` writing to local or `s3://<prefix>-bronze` — `REQ-BRZ-01`.
- Keep Bronze a standalone stage — no Silver/Gold work here — `REQ-BRZ-02`.

## Impact
- Spec deltas: `bronze-ingestion` (ADDED all requirements).
- Code: `src/idp/ingestion/bronze.py`, `src/idp/pipeline.py` (`to_bronze`, `ENTITIES`).
- Decisions: ADR-0001, ADR-0008, ADR-0009.

## Spec Deltas
Satisfies every requirement in `specs/bronze-ingestion`.
