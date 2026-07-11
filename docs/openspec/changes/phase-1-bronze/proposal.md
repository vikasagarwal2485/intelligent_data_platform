# Phase 1 — Bronze Ingestion

## Why
The lakehouse needs an immutable, tenant-partitioned raw layer as the source of
truth for all downstream curation.

## What Changes
- Implement `ingestion/bronze.py` (persist/load raw frames, tenant-partitioned).
- Implement `pipeline.to_bronze(cfg, frames)` writing to local or `s3://<prefix>-bronze`.
- Bronze is a standalone stage — no Silver/Gold work here.

## Impact
- Spec deltas: `bronze-ingestion` (ADDED all requirements).
- Code: `src/idp/ingestion/bronze.py`, `src/idp/pipeline.py` (`to_bronze`, `ENTITIES`).
- Decisions: ADR-0001, ADR-0008, ADR-0009.

## Spec Deltas
Satisfies every requirement in `specs/bronze-ingestion`.
