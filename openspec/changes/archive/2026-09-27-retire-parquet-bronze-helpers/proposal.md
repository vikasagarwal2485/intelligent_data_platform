# Proposal

## Why

`src/idp/ingestion/bronze.py` looks like the Bronze stage, but it isn't. Bronze is
written by `pipeline.to_bronze` through `common/catalog.append_table` into the
Iceberg tables `bronze.<entity>`. That path is fed by `idp consume`
(`streaming/consumer.consume_to_bronze`), `idp run` and `idp migrate-lake`. None of
them import `ingestion/bronze.py`.

What the module holds:
- **`write_landing`**: live. `idp generate` / `make generate` call it to write the
  simulated source file drop, `<landing>/tenant=<id>/<entity>.json` (REQ-BRZ-03).
  It never writes Bronze.
- **`persist_bronze`, `load_bronze`, `ingest`**: dead. These are the Phase-1 Bronze:
  plain local Parquet written straight from the generator, replaced first by
  `to_bronze` and then by Iceberg. Only two tests in `tests/test_ingestion.py`
  still call them.
- **`ENTITIES`**: a second copy of `pipeline.ENTITIES`.

The module docstring still describes Parquet Bronze. The README calls the package
"Bronze landing helpers (bronze.py)". And `docs/traceability.md` still points
REQ-BRZ-01 at `ingestion/bronze.py`, because it copies Phase 1's historical code
pointers. So a reader looking for how Bronze is triggered is sent to code that
never runs.

## What Changes

- **Remove the Phase-1 Parquet helpers:** delete `persist_bronze`, `load_bronze`
  and `ingest`, and their two tests (`test_ingest_and_reload_roundtrip`,
  `test_bronze_is_partitioned_by_tenant`). Iceberg Bronze is covered by
  `tests/test_bronze_iceberg.py` (SCN-BRZ-01.x).
- **Rename the module for what it does:** move `write_landing` to
  `src/idp/ingestion/landing.py` unchanged, and delete `ingestion/bronze.py`.
  Update `idp.ingestion`'s exports and docstring, `cli._cmd_generate`, and
  `tests/test_ingestion.py`.
- **One entity list:** define `ENTITIES` once in `idp.synthetic.generator`, next to
  `SyntheticDataset`, whose fields it names. `pipeline.ENTITIES` re-exports it,
  so every existing `from idp.pipeline import ENTITIES` keeps working. The landing
  writer uses it, and a test pins that the streaming topic map covers the same
  entities.
- **Docs:**
  - README: the package line describes the landing zone and the JSON:API adapter.
  - `docs/gen_traceability.py`: code paths that no longer exist are shown as
    removed, instead of looking current, so REQ-BRZ-01's historical Phase-1
    pointer to `ingestion/bronze.py` reads as retired. Then regenerate
    `docs/traceability.md`.
- **Behaviour unchanged:** `idp generate` writes the same files; Bronze, the stages
  and the CLI commands are untouched.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. REQ-BRZ-01 (Iceberg Bronze via `to_bronze`) and REQ-BRZ-03 (the file landing
zone via `idp generate`) are unchanged, and neither names these modules. This is a
pure refactor, and the change sets `skip_specs: true`.

## Impact

- **Code:**
  - `src/idp/ingestion/bronze.py` (deleted)
  - `src/idp/ingestion/landing.py` (new: `write_landing`)
  - `src/idp/ingestion/__init__.py`
  - `src/idp/cli.py` (`_cmd_generate` import)
  - `src/idp/synthetic/generator.py` (`ENTITIES`)
  - `src/idp/pipeline.py` (re-exports `ENTITIES`)
- **Tests:** `tests/test_ingestion.py` (two Parquet tests removed, imports updated);
  a new check that the streaming topic map covers `ENTITIES`.
- **Docs:** `README.md`, `docs/gen_traceability.py`, `docs/traceability.md`.
- **Removed API:** `idp.ingestion.ingest`, `persist_bronze` and `load_bronze`, and
  the module `idp.ingestion.bronze`. They're used only inside this repository.
- **Not changed:** archived phase proposals, which are the historical record.
