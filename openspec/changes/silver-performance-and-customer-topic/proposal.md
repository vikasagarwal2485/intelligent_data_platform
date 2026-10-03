# Proposal: Silver and Gold performance, and the customer topic

## Why

Three findings from running `raw-bronze-silver-conform` on minikube.

1. **`make silver` takes about 4 minutes** for a small dataset (2 tenants, 150
   agreements and about 35k events each), on a driver and 2 single-core executors.
   The driver log shows where the time goes:
   - about 94 s in Spark stages of **200 tasks** each. `spark.sql.shuffle.partitions`
     is left at Spark's default of 200, and streaming micro-batches (the
     incremental facts) get no adaptive coalescing, so every merge and dedup
     shuffle runs 200 tiny tasks on 2 cores;
   - 19 s conforming the 70k usage events on **one task**. Iceberg reads the 5.9 MB
     raw table as a single split, so the conform UDF runs on one core while the
     other executor is idle;
   - the rest is pod start-up (27 s), the reference conform and the dimensions
     (51 s), and a second, empty commit when a table is first created.
2. **`make gold` takes about 3 minutes** (2 min 49 s, 300 subscribers). Adaptive
   execution already coalesces Gold's shuffles, so the time is elsewhere:
   - the credit and fraud feature tables each compute the same recharge and usage
     aggregates from scratch. The usage aggregation (70k events, with two exact
     distinct counts) runs twice: the 32.5 s and 14.7 s stages;
   - every aggregation first collects its reference time with a separate full scan
     (`_ref_us`): 5 scans, 12 stages, 17.6 s;
   - writing the two tables: 63 s and 21 s; plus pod start-up (about 32 s) and an
     empty first commit per new table.
3. **The Kafka UI shows no message count for the customer topics.** They're
   `compact,delete`, and the UI doesn't count compacted topics, whose offsets aren't
   contiguous. Compaction was there so the former consumer could read every
   customer each run. Since raw Bronze, Silver resolves customers from Bronze
   (REQ-SLV-09), so compaction no longer serves a purpose.

## What Changes

- **Shuffle parallelism per stage.** A Spark profile can set `shuffle_partitions`.
  By default it's twice the profile's executor cores (instances × cores), and
  locally twice the local cores. Every Spark session, the streaming fact batches
  included, uses it.
  - Adaptive query execution with partition coalescing is enabled for the batch
    parts.
  - Streaming checkpoints keep the partition count they started with, so the new
    value takes effect after one `make silver-full-refresh`.
- **Silver conforms each fact batch in parallel.** The raw batch is repartitioned to
  the executor cores before the conform UDF.
- **A new Silver or Gold table is created in one commit**, without the extra empty
  snapshot.
- **Gold computes each shared aggregate once.** The recharge and usage aggregates
  (and their reference times) are built once per run, cached, and used for both
  feature tables. The reference time comes from the same pass instead of a separate
  scan. The feature values don't change (the golden Gold tests guard them).
- **The customer topics go back to the normal delete policy** with the platform
  retention (7 days), like every other topic. The Kafka UI then counts them, and
  purging needs no special case. Existing topics are reconfigured by `make topics`
  (the Strimzi topic operator applies the new config). **No data impact:** Bronze
  holds every customer event.
- **Targets:** on the default minikube volume, `make silver` in about 2 minutes or
  less (most of it pod start-up and the dimensions), and `make gold` in about
  1.5 minutes or less. The results are measured and recorded.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `orchestration`:
  - REQ-ORC-07: a resource profile also sets the stage's shuffle parallelism, which
    defaults to its executor cores.
- `streaming-kafka`:
  - REQ-STR-01: the `customer` topic uses the delete policy like the others; it's no
    longer log-compacted.

## Impact

- **Code:**
  - `transforms/session.py`: shuffle partitions from the profile, and adaptive
    execution;
  - `common/config.py`: profile parsing, if needed;
  - `transforms/conform.py` / `incremental.py`: repartition before conforming;
  - `transforms/lake.py`: create a table from its schema without an empty commit;
  - `transforms/features.py` / `pipeline.to_gold`: shared, cached aggregates and
    reference times for both feature tables;
  - `config/platform.yaml`: profiles documented, with optional overrides.
- **Deploy:**
  - `deploy/platform/kafka/create-topics.sh`: customer topic policy;
  - `purge-topics.sh` comment;
  - re-rendered Spark manifests if the profile shape changes (`make spark-manifests`).
- **Docs:** README (Kafka topics table, Silver and Gold performance notes), `deploy/README.md`,
  `deploy/platform/kafka/README.md`, and the ADR log (a short note on ADR-0031's
  compaction decision).
- **Migration:**
  - `make topics` reconfigures the customer topics;
  - `make silver-full-refresh` once, so the streaming checkpoints pick up the new
    partition count.
- **Out of scope:**
  - faster per-event conforming in Python (each event is schema-validated twice);
  - more executors or cores.
