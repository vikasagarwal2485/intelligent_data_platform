# Proposal

## Why

Every pandas-path lake write (`storage.write_parquet(..., partition_cols=["tenant_id"])`)
**appends** new files to the existing partitions. Re-running Silver, Gold or labels
against the same lake therefore stacks duplicate rows: three runs give three copies.
The hourly `idp_features` DAG makes Silver and Gold grow without bound, which breaks
REQ-SLV-01.2 and REQ-GLD-01 ("one row per subscriber") from the second run on.
Training fans out on duplicated features, and `publish` pushes every duplicate into
TimescaleDB, where `read_score` returns an arbitrary row. The Spark jobs already
overwrite, so the two engines disagree.

## What Changes

- Silver and Gold feature writes **replace** the partitions of the tenants being
  written. Other tenants' partitions are left untouched.
- The labels store is replaced per tenant on re-seed.
- Score tables stay **append-only** by design (REQ-ML-07 history), and each run
  contributes exactly one row per subscriber.
- `publish` serves only the **latest** score run per tenant, and every serving table
  holds exactly one row per `(tenant_id, service_id)`, enforced by a unique index.
  The lookup (REQ-SRV-04) is therefore deterministic.
- Bronze keeps append semantics (immutable raw log); Silver de-duplicates it.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `silver-curation`: add REQ-SLV-06, idempotent Silver writes.
- `gold-features`: add REQ-GLD-04, idempotent Gold feature writes.
- `ml-models`: add REQ-ML-08, idempotent labels store and one row per subscriber per
  score run.
- `serving-api`: add REQ-SRV-05, the serving store holds the latest score per
  subscriber.

## Impact

- Code: `src/idp/common/storage.py` (write modes), `src/idp/pipeline.py` (callers
  choose replace or append; publish selects the latest run),
  `src/idp/serving/timescale.py` (unique index).
- Data: existing lakes with duplicates need a one-off clean rebuild
  (`make minio-reset` and re-run, or rewrite each table once with the new code).
- Depends on `sync-specs-with-implementation` being archived first (REQ-SRV-04,
  REQ-ML-07 history wording).
