# Tasks

## 1. Verify the deltas against the code

- [x] 1.1 For each MODIFIED/ADDED requirement in this change's `specs/`, confirm the cited behaviour in code (`pipeline.py`, `cli.py`, `serving/api.py`, `serving/timescale.py`, `mcp/tools.py`, `streaming/*`, `idp_medallion_dag.py`, `install-infra.sh`, `Makefile`). Done when every requirement's behaviour has been confirmed in the code, and any delta that does not match the code has been corrected in this change's `specs/`
- [x] 1.2 Add tests for the new scenarios that lack one: SCN-BRZ-03.1/03.2 (landing layout), SCN-ML-06.4/06.5 (empty-join error, duplicate labels do not fan out), SCN-ML-07.3/07.4 (skip missing model, `scored_at` per run), SCN-SRV-01.4 (nothing to publish), SCN-SRV-04.2/04.3 (lookup 503/404), SCN-MCP-06.4 (unreachable lake), SCN-STR-01.2 (`fees` topic mapping). Verify with `pytest -q`, which must be green

## 2. Housekeeping

- [x] 2.1 Edit the Purpose of `openspec/specs/gold-features/spec.md` to drop "(and optional scores)" and point scoring to REQ-ML-07. Verify with `openspec show gold-features --type spec`
- [x] 2.2 Update the `idp mcp` help text in `src/idp/cli.py` and the `mcp` target in `Makefile` from "5 Silver analytics tools" to "7 analytics tools (5 Silver, 2 Gold scores)". Verify with `idp --help` and `make help`
- [x] 2.3 Update the `src/idp/pipeline.py` module docstring: Gold is features-only, and list the `score`/`labels` stages. Verify by reading the docstring
- [x] 2.4 Fix `docs/gen_traceability.py` to resolve `openspec/specs` and `openspec/changes` from the repo root, and fix the self-referencing path strings. The 16 `phase-*` changes are archived before this change (outside its scope), so the generator must read phase proposals from `openspec/changes/archive/*-phase-*/proposal.md` and link to those archived paths. Verify that `python docs/gen_traceability.py` regenerates `docs/traceability.md` without errors, with every phase link resolving to an archived proposal

## 3. Archive

- [x] 3.1 Archive this change (`openspec archive sync-specs-with-implementation`). Verify that `openspec validate --specs --strict` passes and that `openspec list --specs` shows REQ-GLD-03 is gone
- [x] 3.2 Regenerate `docs/traceability.md`. Verify that the coverage line reflects the new requirement and scenario counts
