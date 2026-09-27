# Proposal

## Why

`idp generate` writes the synthetic source data as files to a landing zone
(`<landing>/tenant=<id>/<entity>.json`), but nothing reads them. `idp produce` and
`idp run` call the generator directly, so the landing zone is a dead end. REQ-BRZ-03
even says the landing zone is "independent of the Kafka path".

The architecture plan (§3, §3.2) describes a different source path. Files land per
tenant, a pickup step (NiFi) checks and stamps them and publishes them to the
tenant's Kafka topics, and the consumer writes Bronze. Today `idp produce` stands in
for both the source and NiFi, so the path from files to Kafka is never exercised.
The data that reaches Kafka also can't be inspected or replayed before it's
published.

## What Changes

- **`idp produce --from-landing`:** publishes the records in the landing zone instead
  of generating new ones.
  - It reads `tenant=<id>/<entity>.json` one tenant at a time, and publishes the
    records exactly as stored. They produce the same Kafka messages (topic, key and
    JSON value) that in-memory `idp produce` sends for the same data.
  - It plays the pickup role: a file whose folder names an unconfigured tenant, or
    that contains any record whose `tenant_id` doesn't match its folder, is **not
    published** at all. It's counted as rejected, the other files are still
    published, and the command exits non-zero if anything was rejected.
  - Without the flag, `idp produce` is unchanged: it generates in memory and
    publishes.
- **A storage-aware landing zone:** the local `storage.landing` directory as today,
  or the bucket `s3://<prefix>-landing` when S3 is enabled (`idp-landing` on
  MinIO). Both `idp generate` and `idp produce --from-landing` use it.
  - `idp generate` writes one tenant at a time, like `produce`, so it fits the same
    pod memory.
  - The file layout and contents are unchanged.
- **Cluster path:**
  - The `idp-landing` bucket is created by `install-infra.sh`,
    `make minio-mkbuckets` and `make minio-reset`.
  - There's a new `idp-generate` Job, with the same `IDP_SUBSCRIBERS`/`IDP_DAYS` as
    the produce and labels Jobs.
  - There's a new `idp-produce-from-landing` Job, which runs
    `idp produce --from-landing`.
- **`make stream-from-landing`** runs the landing path end to end with one command:
  1. `idp generate`: synthetic source files into the landing zone (`idp-landing`)
  2. `idp produce --from-landing`: the landing files, tenant-checked, onto the
     Kafka topics
  3. `idp consume`: Kafka into Iceberg Bronze

  Each step runs as a Job in `idp-app`, starts only after the previous one has
  completed, and the target stops at the first failed step. A rejected landing file
  fails the produce step, so consume doesn't run. The target then prints the
  landing, produce and consume summaries.
  - `make stream-replay` re-publishes the existing landing files (steps 2 and 3)
    without regenerating them.
  - **`make stream` is unchanged:** in-memory `idp produce` (the existing produce
    Job), then consume. Both paths stay available, and both end in the same Bronze
    for the same data.
- **Docs:** README and `deploy/README.md` describe the landing-zone path, and how to
  inspect or replay the landing files.

Non-goals:
- NiFi itself, a dead-letter topic, and Avro on the wire. Rejected files are
  reported, not routed.
- Schema validation beyond the tenant check.
- Changing the consumer, Bronze or later stages.
- Making `idp run` read the landing zone.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `bronze-ingestion`: REQ-BRZ-03. The landing zone becomes the source of the Kafka
  path, when `produce` runs with `--from-landing`, and is storage-aware (local or
  `s3://<prefix>-landing`).
- `streaming-kafka`: REQ-STR-01. The producer can publish from the landing zone,
  with a tenant check per file and the same messages as in-memory production.
- `orchestration`: REQ-ORC-01. `idp generate` and `idp produce --from-landing`
  join the runnable stages, and streaming ingestion is runnable with one command in
  two variants: `make stream` (in-memory) and `make stream-from-landing` (landing
  path).
- `platform-deployment`: REQ-PLT-02. The install creates the `idp-landing` bucket
  alongside the lake buckets. In passing, SCN-PLT-02.3 is corrected to the four
  IDP DAGs; `idp_maintenance` has existed since iceberg-incremental-lakehouse.

## Impact

- **Code:**
  - `src/idp/ingestion/landing.py`: storage-aware write per tenant, and a reader
    that yields each landing file's records
  - `src/idp/streaming/producer.py`: a record-level publish that
    `produce_to_kafka` delegates to
  - `src/idp/cli.py`: `produce --from-landing`, and `generate` per tenant
  - `src/idp/common/storage.py`: `landing_uri(cfg)`
- **Deploy:**
  - `deploy/app/jobs/generate-job.yaml` and
    `deploy/app/jobs/produce-from-landing-job.yaml` (new);
    `deploy/app/jobs/produce-job.yaml` is unchanged
  - `Makefile` (`stream-from-landing`, `stream-replay`, `generate-job`, bucket
    targets; `stream` unchanged)
  - `deploy/platform/install-infra.sh` (the `idp-landing` bucket)
- **Tests:**
  - landing round trip, local and S3-shaped paths through fsspec
  - message equivalence between the in-memory and landing paths (fake producer)
  - rejection of mismatched and unknown-tenant files
  - the CLI's exit status
- **Operational:** `make stream-from-landing` always regenerates the landing zone
  first. `make stream-replay` re-publishes whatever is there; Bronze and Silver
  de-duplicate as before.
