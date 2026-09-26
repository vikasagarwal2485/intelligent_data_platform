# Design

## Context

`storage.write_parquet` calls `DataFrame.to_parquet(uri, partition_cols=[...])`.
With partition columns, pandas delegates to pyarrow's dataset writer, whose default
`existing_data_behavior="overwrite_or_ignore"` writes new, uniquely named files next
to the old ones. That is an append. `_read_labels` already works around this by
de-duplicating. The Spark jobs use `mode("overwrite")`. The comment in
`write_parquet` warns that an earlier bucket "ensure" step poisoned the fsspec cache,
so any change here must stay a plain `to_parquet` call. See proposal.md — Why.

## Goals / Non-Goals

**Goals:**
- A replace-by-tenant-partition write mode, used by Silver, Gold features and labels.
- An explicit append mode for score history.
- Publish reads the latest run per tenant; the serving store enforces uniqueness.

**Non-Goals:**
- Bronze semantics. Bronze stays append-only as a raw log, and Silver already
  de-duplicates facts on `(tenant_id, id)` and the dimension on `service_id`.
- Iceberg/Nessie table formats, which would give atomic snapshot commits. That
  belongs to the Nessie migration.
- Retention or compaction of score history.
- Spark write modes; covered by `spark-engine-parity`.

## Decisions

- **`mode` parameter on `write_parquet`: `"replace"` (default) or `"append"`.**
  - `replace` passes `existing_data_behavior="delete_matching"` through `to_parquet`
    to pyarrow. This deletes only the partition directories being written
    (`tenant_id=<t>`), which is exactly per-tenant replace.
  - `append` keeps today's behaviour.
  - Making `replace` the default fixes every caller except the one that should
    append (`to_scores`), which passes `mode="append"` explicitly.
  - Alternative: delete the table prefix with `fs.rm` first. Rejected because it is
    not per-tenant, and it reintroduces the extra-filesystem pattern that broke S3
    writes before.
- **Non-partitioned writes** (none today) use pandas' normal file overwrite.
- **Publish selects the latest run.** `publish_serving` filters each score table to
  rows where `scored_at == max(scored_at)` within each tenant before handing it to
  `timescale.publish_gold`. Rows without `scored_at` (legacy data) are treated as
  one run.
  - Alternative: have the Score stage also write a separate "latest" table.
    Rejected because it duplicates storage and still needs selection logic.
- **Unique index.** `publish_frame` creates `UNIQUE INDEX ... (tenant_id, service_id)`
  instead of a plain index. A violation raises inside the transaction, so the
  previous table survives (SCN-SRV-05.2).
- **Keep the de-duplication in `_read_labels`** as defence-in-depth for lakes
  written before this change.

## Risks / Trade-offs

- [`delete_matching` on s3fs/MinIO may behave differently from the local filesystem]
  → Add an integration check (task 4.2) against MinIO. If it fails, fall back to
  explicitly removing `…/tenant_id=<t>/` through the same filesystem object pyarrow
  uses.
- [Replace is not atomic: a reader during the write may see a missing partition] →
  Acceptable for batch stages. Iceberg removes this later.
- [Existing duplicated lakes] → Migration step below.
- [Score history grows forever] → Out of scope; note it for a later retention change.

## Migration Plan

1. Deploy the new image (`make image`).
2. Either `make minio-reset` and re-run the pipeline, or run `idp silver`,
   `idp gold` and `idp labels` once. Replace mode rewrites every tenant partition
   those stages touch, which removes the stacked copies.
3. `idp publish` rebuilds the serving tables with unique indexes.
4. Rollback: the previous image appends again. The data stays readable.
