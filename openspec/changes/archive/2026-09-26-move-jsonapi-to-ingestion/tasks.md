# Tasks

## 1. Move the adapter

- [x] 1.1 `git mv src/idp/curation/jsonapi.py src/idp/ingestion/jsonapi.py`, and update its module docstring to describe it as the JSON:API source adapter of the ingestion layer (REQ-BRZ-04), dropping the "NiFi/Spark promoting into Bronze/Silver" wording. Verify with `python -c "from idp.ingestion import jsonapi"`
- [x] 1.2 Export `normalize_money`, `strip_uuid_wrapper`, `unwrap_usage_event` and `unwrap_recharge_event` from `src/idp/ingestion/__init__.py`. Verify with `python -c "from idp.ingestion import unwrap_usage_event"`
- [x] 1.3 Delete the now-empty `src/idp/curation/` package. Verify that `grep -rn "idp.curation\|curation/" src tests pipelines` returns nothing

## 2. Importers and docs

- [x] 2.1 Update the imports in `tests/test_jsonapi.py` and `tests/test_transforms_runtime.py`, and the `strip_uuid` docstring reference in `src/idp/transforms/tokens.py`, to `idp.ingestion.jsonapi`. Tag `test_jsonapi.py` with the REQ-BRZ-04 scenarios (04.1 data usage, 04.2 voice, 04.3 recharge, 04.4 missing money). Verify that `pytest tests/test_jsonapi.py tests/test_transforms_runtime.py` is green
- [x] 2.2 Add a test for the modified SCN-SLV-02.2: a Bronze agreement whose `account_id` is `"UUID(<id>)"` curates to a `dim_subscriber` row carrying the account's attributes (the join worked). Verify that it passes in `tests/test_transforms_silver.py`
- [x] 2.3 Fix `README.md`: in the project tree, drop `curation/` and list `ingestion/` (Bronze landing + JSON:API adapter) and `transforms/` (Spark Silver/Gold). In the Silver section, point "JSON:API unwrap" at the ingestion adapter and describe what Silver itself normalizes. Verify that `grep -n "curation" README.md` returns no stale reference

## 3. Verification

- [x] 3.1 Run the full `pytest -q`. Verify that it is green, with the same test count plus the new SCN-SLV-02.2 test
