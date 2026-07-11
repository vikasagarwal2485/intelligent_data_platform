# Phase 0 — Foundation

## Why
Every capability depends on configuration, tenant isolation, GDPR PII handling,
lake storage, a synthetic data source, and a test/CI harness. Build these first.

## What Changes
- Create the `idp` package skeleton (`src/idp`, `pyproject.toml`, editable install).
- Implement `common/config.py` (typed config + `IDP_*` env overrides),
  `common/tenancy.py` (stamp/scope/guards), `common/pii.py` (per-tenant tokenize +
  consent), `common/storage.py` (local/S3 Parquet, per-layer bucket URIs).
- Implement `synthetic/generator.py` (deterministic, multi-tenant, labelled).
- Add `config/platform.yaml` (tenants, storage, kafka, s3, synthetic, features,
  credit, fraud) and `config/avro/*.avsc` contracts.
- Add `tests/` and a CI workflow running `make test`.

## Impact
- Spec deltas: `foundation` (ADDED all requirements), `synthetic-data` (ADDED all).
- Code: `src/idp/common/*`, `src/idp/synthetic/*`, `config/*`, `tests/*`,
  `pyproject.toml`, `Makefile`, `.github/workflows/ci.yml`.
- Decisions: ADR-0005, ADR-0006, ADR-0008, ADR-0010, ADR-0013, ADR-0014.

## Spec Deltas
Satisfies every requirement in `specs/foundation` and `specs/synthetic-data`.
