# Proposal

## Why

`src/idp/curation/jsonapi.py` parses TM Forum JSON:API envelopes, the format of the
real source documents in `data_samples/`. Since `spark-single-engine` it is the only
module left in the `curation` package, and no pipeline stage calls it:

- The synthetic generator and the Kafka producer emit records that are already
  flat.
- Silver does its own id canonicalization and money normalization, in Spark.

The code and the spec disagree about where envelope unwrapping belongs:

- REQ-SLV-02 says the Silver stage "SHALL unwrap JSON:API envelopes", but Silver
  never sees an envelope.
- Unwrapping an envelope into a canonical record is a source-format concern of
  **ingestion**. It happens where data enters the platform, before Bronze, so the
  module belongs in `idp.ingestion`, not in a curation package or `common/`.

## What Changes

- Move `src/idp/curation/jsonapi.py` to `src/idp/ingestion/jsonapi.py` (the
  JSON:API source adapter, next to the Bronze landing code) and delete the now-empty
  `src/idp/curation` package. Behaviour is unchanged. **BREAKING** for any external
  importer of `idp.curation`; there are none in the repo besides tests.
- Update the importers: `tests/test_jsonapi.py`, `tests/test_transforms_runtime.py`
  and the `idp.transforms.tokens` docstring. Fix the stale `README.md` lines that
  still describe `curation/` as "JSON:API unwrap + canonical tables (`silver.py`)".
- Specs:
  - **MODIFIED and RENAMED** REQ-SLV-02 becomes "Id canonicalization and
    normalization". Silver strips `UUID(...)` wrappers from relationship ids,
    computes `gross_amount = net_amount + tax_amount`, normalizes timestamps to UTC
    and de-duplicates on natural keys. The envelope-unwrap clause is removed.
  - **ADDED** REQ-BRZ-04, "JSON:API source adapter": ingestion provides the adapter
    that flattens TM Forum JSON:API usage and recharge envelopes into canonical
    records.
- Non-goal: calling the adapter from `idp consume`. Feeding real JSON:API envelopes
  into Bronze belongs with the Bronze rework in the planned
  `iceberg-incremental-lakehouse` change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `silver-curation`: REQ-SLV-02 is renamed and narrowed to what Silver does: id
  canonicalization, money, UTC and de-duplication.
- `bronze-ingestion`: new REQ-BRZ-04, the JSON:API source adapter.

## Impact

- **Code:**
  - `src/idp/ingestion/jsonapi.py` (moved; docstring updated)
  - `src/idp/ingestion/__init__.py` (exports the adapter)
  - `src/idp/curation/` removed
  - `src/idp/transforms/tokens.py` (docstring only)
- **Tests:** import paths only. No behaviour change, and the same assertions against
  `data_samples/`.
- **Docs:** `README.md` project tree and the Silver section.
- **No runtime, data or deployment impact.** Nothing in the pipeline imports the
  module.
