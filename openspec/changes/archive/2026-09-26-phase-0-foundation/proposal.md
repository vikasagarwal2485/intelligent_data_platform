# Phase 0 — Foundation

## Why
Every capability depends on configuration, tenant isolation, GDPR PII handling,
lake storage, a synthetic data source, and a test/CI harness. Build these first.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- Create the `idp` package skeleton (`src/idp`, `pyproject.toml`, editable install) — enables `REQ-FND-01`…`REQ-FND-04`.
- Implement `common/config.py` (typed config + `IDP_*` env overrides) — `REQ-FND-01`.
- Implement `common/tenancy.py` (stamp/scope/guards) — `REQ-FND-02`.
- Implement `common/pii.py` (per-tenant tokenize + consent) — `REQ-FND-03`.
- Implement `common/storage.py` (local/S3 Parquet, per-layer bucket URIs) — `REQ-FND-04`.
- Implement `synthetic/generator.py` (deterministic, multi-tenant, labelled) — `REQ-SYN-01`, `REQ-SYN-02`, `REQ-SYN-03`.
- Add `config/platform.yaml` and `config/avro/*.avsc` contracts — `REQ-FND-01` (config).
- Add `tests/` and a CI workflow running `make test` — verifies `REQ-FND-*`, `REQ-SYN-*`.

## Impact
- Spec deltas: `foundation` (ADDED all requirements), `synthetic-data` (ADDED all).
- Code: `src/idp/common/*`, `src/idp/synthetic/*`, `config/*`, `tests/*`,
  `pyproject.toml`, `Makefile`, `.github/workflows/ci.yml`.
- Decisions: ADR-0005, ADR-0006, ADR-0008, ADR-0010, ADR-0013, ADR-0014.

## Spec Deltas
Satisfies every requirement in `specs/foundation` and `specs/synthetic-data`.
