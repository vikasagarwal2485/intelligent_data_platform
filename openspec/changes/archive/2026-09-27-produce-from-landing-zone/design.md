# Design

## Context

See proposal.md (Why). The facts that shape the approach:
- **The producer:** `produce_to_kafka(dataset, …)` iterates `TABLE_TO_SUFFIX`,
  turns each frame into dict records (`df.to_dict("records")`), and sends them
  with `value_serializer=json.dumps(…, default=str)`. The key comes from
  `service_id`, else `msisdn`, else `id`.
- **The landing writer:** `write_landing` serializes the same kind of dict records
  with the same `json.dumps(…, default=str)`. So a landing file already holds
  exactly the JSON values the producer would send. Reading it back and sending the
  dicts yields the same bytes. Timestamps, numpy scalars and `NaN` were all
  stringified or encoded the same way on the way in.
- **Memory:** `idp produce` generates and publishes one tenant at a time to fit its
  2 Gi pod. `idp generate` currently builds every tenant at once
  (`generate_for_tenants`), which is exactly the concatenation of the per-tenant
  `generate()` results.
- **Storage:** the lake already uses fsspec/s3fs with `storage.storage_options(cfg)`,
  and each layer has its own bucket `<prefix>-<layer>` (ADR-0008).
- **Labels:** the labels Job regenerates labels from `IDP_SUBSCRIBERS`/`IDP_DAYS`.
  Any Job that generates source data must use the same values.

## Goals / Non-Goals

**Goals:**
- The landing zone becomes a real, inspectable and replayable source for the
  Kafka path, locally and in the cluster.
- The messages on Kafka are identical whichever source the producer used.
- Bad files are held back and reported, never partly published.

**Non-Goals:**
- A dead-letter topic and NiFi.
- Streaming parse of very large files. A file holds one tenant's entity, the same
  unit the in-memory path holds.

## Decisions

### 1. The landing location is resolved like a lake layer
`storage.landing_uri(cfg)` returns `s3://<prefix>-landing` when S3 is enabled, and
`cfg.storage.landing` otherwise. Every landing read and write goes through
`fsspec.core.url_to_fs(uri, **storage_options)`, so one code path covers the local
directory and MinIO/S3.
- *Alternative:* a `tenant=` prefix inside `idp-bronze`. Rejected: raw source files
  aren't Bronze, and a separate bucket keeps policies separate (ADR-0008). Bronze
  archiving and orphan cleanup must never touch source files.

### 2. The landing module gains a writer per tenant and a checking reader
In `ingestion/landing.py`:
- **`write_landing(dataset, landing, *, storage_options=None)`** writes one dataset
  (one or several tenants) with the same layout and bytes as today.
  `idp generate` loops over the configured tenants: `generate(tenant)`, then
  `write_landing`, then drop the dataset. Peak memory is one tenant.
- **`read_landing(landing, tenants, *, storage_options=None)`** walks
  `tenant=<id>/` folders in sorted order and yields one item per file:
  `(tenant, entity, records | None, reason | None)`.
  - The entities are read in `TABLE_TO_SUFFIX` order, the same order as the
    in-memory path.
  - A file is rejected, with `records=None` and a reason, when:
    - its tenant folder isn't in the configured tenants (`unknown tenant`)
    - the file isn't `<known entity>.json` (`unknown entity`)
    - the JSON isn't an array of objects (`invalid json`)
    - any record's `tenant_id` differs from the folder (`tenant mismatch: N records`)
  - Only one file is held in memory at a time.

### 3. The producer publishes records; the dataset path delegates
`producer.publish_records(items, bootstrap, *, topic_prefix, acks)` takes an
iterable of `(table, records)` pairs. It owns the `KafkaProducer`, the topic and
key rules, flush and close, and returns counts per topic.
- `produce_to_kafka(dataset, …)` becomes a thin wrapper that feeds
  `(table, df.to_dict("records"))` pairs, so the existing behaviour and tests are
  unchanged.
- The landing path feeds the accepted `(entity, records)` pairs from `read_landing`.
- Both pass their dicts through the same serializer. That's what makes the messages
  identical: the landing dicts are the already-serialized form of the in-memory
  dicts. SCN-STR-01.3 pins it with a fake producer that compares
  `(topic, key, serialized value)` across the two paths.

### 4. The CLI and exit status
- `idp produce --from-landing` uses `read_landing(landing_uri(cfg), cfg.tenant_ids)`.
  It publishes the accepted files through `publish_records`, one `KafkaProducer`
  per tenant as today, and prints the per-tenant counts and a `rejected` list
  (path and reason).
- It exits with status 1 when anything was rejected, after publishing everything
  that was accepted. It exits with status 1 as well when the landing zone holds no
  tenant folders, so an empty landing zone isn't a silent success.
- `idp generate` prints where it wrote (the local path or `s3://…`).

### 5. Cluster wiring
- **Bucket:** `idp-landing` is added to the bucket loops in `install-infra.sh`,
  `make minio-mkbuckets` and `make minio-reset`.
- **`deploy/app/jobs/generate-job.yaml`:** `idp generate` with the S3 env,
  `IDP_SUBSCRIBERS=150` and `IDP_DAYS=14`. These must stay equal to the produce and
  labels Jobs, and a comment in all three says so. It uses the produce Job's
  resources.
- **`produce-from-landing-job.yaml` (new, Job `idp-produce-from-landing`):**
  `["idp", "produce", "--from-landing"]`, with the Kafka and S3 env. It needs no
  synthetic env. `produce-job.yaml` (in-memory `idp produce`) is unchanged.
- **`make stream-from-landing`:**
  1. Delete any previous `idp-generate`, `idp-produce-from-landing` and
     `idp-consume` Jobs.
  2. Apply `generate-job.yaml` and wait for complete, then apply
     `produce-from-landing-job.yaml` and wait for complete, then apply
     `consume-job.yaml` and wait for complete.
  3. Print each Job's log summary.

  `make` stops at the first failing command, so a failed step (including a
  rejected landing file, which is exit 1) stops the chain. The failed Job's pod is
  kept for its logs.
  - `make generate-job` runs only step 1.
  - `make stream-replay` runs only the produce and consume steps, re-publishing
    the existing landing files.
- **`make stream`** keeps its recipe (produce Job, then consume Job). The two targets
  share the consume Job and the Kafka topics, so they must not run concurrently. The
  help text says so, as it does for the Silver runs.

### 6. REQ-ORC-01 overlaps with `distributed-spark-runtime`
Both changes modify REQ-ORC-01. `distributed-spark-runtime` adds SCN-ORC-01.4 and
rewords the Spark-stage paragraph; this change adds `idp generate` /
`idp produce --from-landing` to the stage list and SCN-ORC-01.5–01.7. The scenario
ids don't collide. Whichever change is archived second must rebase its ORC-01 delta
on the main spec, keeping the other change's edits. The archive's scenario check
enforces this.

## Risks / Trade-offs

- **[Risk]** Per-tenant writes could differ in bytes from today's all-tenant writes
  if concatenation changes a column's type (e.g. an all-null column in one tenant).
  → The task compares the local `idp generate` output byte for byte with the
  pre-change baseline under the default configuration. Any difference is a bug,
  or is documented explicitly.
- **[Risk]** A stale landing zone is re-published after the generator's parameters
  change, while the labels come from the new parameters. → `make stream-from-landing`
  always regenerates first; only `make stream-replay` reuses the files, and its help
  text says so.
- **[Risk]** `make stream` and `make stream-from-landing` run concurrently and
  interleave on the same topics and consumer group. → Both help texts say to run
  one at a time. The consumer's commit-after-append keeps Bronze consistent either
  way, and Silver de-duplicates.
- **[Trade-off]** Exit status 1 on any rejected file makes `make stream-from-landing`
  stop before consume. That's intended: bad source data should stop the run visibly, and the
  good files are already on Kafka for a manual consume.

## Migration Plan

1. `make minio-mkbuckets`, which creates `idp-landing`, then `make image`.
2. `make stream-from-landing` (generate → produce --from-landing → consume). Then
   `make silver`: with the same deterministic dataset, it processes the new Bronze
   snapshot but the Silver row counts don't change (the insert-if-absent `MERGE`),
   which shows that the landing path carries the same records.
3. Rollback: use `make stream`. The in-memory path is unchanged, so rolling back
   only means not using the new target.
