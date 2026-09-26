# Phase 6 — Serving API

## Why
Expose the credit and fraud models for on-demand, tenant-scoped scoring.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- Implement `serving/api.py`: FastAPI app factory with `/health`, `/credit-limit`,
  `/fraud-score`; lazy model loading — `REQ-SRV-02`.
- Enforce tenant scoping and HTTP 503 when models are absent — `REQ-SRV-03`.
- Implement `pipeline.publish_serving` (Gold → TimescaleDB `gold.*`) as `idp publish`
  + a `/serving/{table}/{tenant}/{service_id}` lookup — `REQ-SRV-01`.
- Add `idp serve` CLI and a `Dockerfile` running the service — `REQ-SRV-02`.

## Impact
- Spec deltas: `serving-api` (ADDED all requirements).
- Code: `src/idp/serving/api.py`, `src/idp/cli.py` (`serve`), `Dockerfile`.
- Decisions: ADR-0005 (tenant scoping), ADR-0010.

## Spec Deltas
Satisfies every requirement in `specs/serving-api`.
