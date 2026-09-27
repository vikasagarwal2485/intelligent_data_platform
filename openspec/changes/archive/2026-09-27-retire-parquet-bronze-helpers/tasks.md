# Tasks

## 1. One entity list

- [x] 1.1 Define `ENTITIES` in `src/idp/synthetic/generator.py` next to `SyntheticDataset` (the eight entity tables, in the current order), and make `pipeline.ENTITIES` a re-export of it. Verify with a test that `ENTITIES` equals the `SyntheticDataset` fields minus `labels` and the keys of `streaming.TABLE_TO_SUFFIX`, and that `idp.pipeline.ENTITIES is idp.synthetic.generator.ENTITIES`

## 2. Landing module and retirement

- [x] 2.1 Create `src/idp/ingestion/landing.py` holding `write_landing` (behaviour unchanged, iterating `generator.ENTITIES`), point `cli._cmd_generate` and `idp.ingestion` (exports and docstring: landing zone plus JSON:API adapter) at it, and delete `src/idp/ingestion/bronze.py` with `ingest`, `persist_bronze` and `load_bronze`. Verify that `from idp.ingestion import write_landing` works, `import idp.ingestion.bronze` raises `ModuleNotFoundError`, and `python -X importtime -m idp.cli generate --help` doesn't import `sklearn`
- [x] 2.2 Update `tests/test_ingestion.py`: import from `idp.ingestion.landing`, keep the landing tests (`test_write_landing_partitions_by_tenant`, `test_landing_has_one_file_per_tenant_entity`, `test_generate_command_writes_landing_not_bronze`), and remove `test_ingest_and_reload_roundtrip` and `test_bronze_is_partitioned_by_tenant`. Verify with `pytest -q tests/test_ingestion.py tests/test_bronze_iceberg.py`
- [x] 2.3 Repository-wide search for `ingestion.bronze`, `ingestion/bronze`, `persist_bronze`, `load_bronze` and `bronze.ingest` in `src`, `tests`, `pipelines`, `skills`, `deploy`, `docs` (except the generated traceability) and `README.md`. Verify that nothing is found outside `openspec/changes/archive/`

## 3. Docs

- [x] 3.1 README: replace the `ingestion/` line ("Bronze landing helpers (bronze.py)") with the landing zone (`landing.py`, `idp generate`) and the JSON:API adapter, and say that Bronze is written by `pipeline.to_bronze`. Verify by reading
- [x] 3.2 `docs/gen_traceability.py`: render a code path that exists neither at the repository root nor under `src/idp/` as `` `path` (removed) ``; leave paths with a scheme (`s3://`) or annotations as they are. Regenerate `docs/traceability.md` and verify that REQ-BRZ-01 shows `ingestion/bronze.py` and `src/idp/ingestion/bronze.py` as removed, next to the current `src/idp/pipeline.py`, and that no other requirement's code list changed apart from paths that are really gone (review `git diff docs/traceability.md`) — Done. REQ-BRZ-01 shows `ingestion/bronze.py` (removed); `src/idp/ingestion/bronze.py` never appears, because Phase 1 cites it on a line without a REQ id. The only other lines that changed are paths that are really gone: `features/fraud_features.py` (×2), `spark-gold-job.yaml`, `curation/jsonapi.py`. Non-path tokens (prose, routes, `s3://`, templated paths) are left as they are.

## 4. Verification

- [x] 4.1 `pytest -q` is green, and `idp generate` against a temporary config writes the same landing files as before (the existing `test_generate_command_writes_landing_not_bronze` plus a before/after file listing) — Done 2026-09-27: 204 passed (205 − 2 retired Parquet tests + 1 entity-list test); the `idp generate` output is byte-identical before and after (16 files, SHA-256).
