# Design

## Context

See proposal.md (Why). The facts that shape the approach:
- `pipeline.py` imports the ML models (scikit-learn) at module level, so importing
  anything from `idp.pipeline` loads the ML stack. `idp generate` today imports
  only `idp.ingestion.bronze` and `idp.synthetic.generator`, which are light.
- The same eight entities appear in `pipeline.ENTITIES`, `ingestion/bronze.ENTITIES`,
  the field list of `SyntheticDataset` (plus `labels`), and the keys of
  `streaming.TABLE_TO_SUFFIX`.
- `docs/gen_traceability.py` builds each requirement's "Code:" list from the
  backticked paths in the phase proposals (active and archived). REQ-BRZ-01 gets
  `src/idp/ingestion/bronze.py` from Phase 1's archived proposal.

## Goals / Non-Goals

**Goals:**
- No code path suggests that `ingestion/` writes Bronze.
- One definition of the entity list.
- Generated docs stay truthful without rewriting history.

**Non-Goals:**
- Changing the landing-zone format or `idp generate`'s behaviour.
- A reader for the landing zone. It stays a simulation of a source file drop
  (REQ-BRZ-03).
- Moving the TM Forum JSON:API adapter (`ingestion/jsonapi.py`), which stays where
  it is.

## Decisions

### 1. `ingestion/landing.py`, and no `bronze.py` at all
`write_landing` moves as is to `ingestion/landing.py`, and `ingestion/bronze.py` is
deleted, not trimmed. A `bronze.py` holding only the landing writer would keep the
misleading name.
- *Alternative:* keep `bronze.py` with only `write_landing`. Rejected: that's the
  confusion this change removes.

### 2. No compatibility aliases
The removed names (`ingest`, `persist_bronze`, `load_bronze`, the module
`idp.ingestion.bronze`) aren't re-exported. The package isn't published, and a
repository-wide search is the check, as in `rename-produce-to-produce-to-kafka`.

### 3. `ENTITIES` lives in `idp.synthetic.generator`
It names the entity tables of `SyntheticDataset`. Defining it beside the dataclass
keeps it light (pandas/numpy only), so `idp generate` doesn't import the ML stack.
- `pipeline.ENTITIES` becomes a re-export
  (`from idp.synthetic.generator import ENTITIES`), so the existing imports in
  tests and `migration.py` are unchanged.
- A test asserts that `ENTITIES` equals the `SyntheticDataset` fields minus
  `labels`, and equals the keys of `streaming.TABLE_TO_SUFFIX`. The copies can't
  drift silently.
- *Alternative:* the landing writer imports `idp.pipeline.ENTITIES`. Rejected: it
  pulls scikit-learn into `idp generate`.

### 4. Traceability marks removed paths instead of editing archives
Archived phase proposals are the historical record and stay untouched.
`gen_traceability.py` checks each code path against the repository. A path that
doesn't exist is rendered as `` `src/idp/ingestion/bronze.py` (removed) ``. Paths
that aren't files, such as `s3://…` or the `src/idp/pipeline.py (to_bronze, …)`
notes, are shown as today.
- *Alternative:* a per-requirement override table in the generator. Rejected as more
  mechanism than one stale pointer needs. The Phase-1 pointer also lists
  `src/idp/pipeline.py`, which stays correct.

## Risks / Trade-offs

- **[Risk]** Something outside `src`/`tests` imports `idp.ingestion.bronze`, e.g. a
  notebook or a skill. → A repository-wide search in the tasks (`src`, `tests`,
  `pipelines`, `skills`, `docs`, `deploy`, README) is the acceptance check.
- **[Risk]** Traceability can't tell a moved file from a deleted one. → It says
  "removed", which is true of the path, and the requirement still links its spec and
  phases.
