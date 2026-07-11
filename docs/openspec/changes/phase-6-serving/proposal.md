# Phase 6 — Serving API

## Why
Expose the credit and fraud models for on-demand, tenant-scoped scoring.

## What Changes
- Implement `serving/api.py`: FastAPI app factory with `/health`,
  `/credit-limit`, `/fraud-score`; lazy model loading from the artifact store;
  HTTP 503 until models exist; tenant scoping.
- Add `idp serve` CLI and a `Dockerfile` running the service.

## Impact
- Spec deltas: `serving-api` (ADDED all requirements).
- Code: `src/idp/serving/api.py`, `src/idp/cli.py` (`serve`), `Dockerfile`.
- Decisions: ADR-0005 (tenant scoping), ADR-0010.

## Spec Deltas
Satisfies every requirement in `specs/serving-api`.
