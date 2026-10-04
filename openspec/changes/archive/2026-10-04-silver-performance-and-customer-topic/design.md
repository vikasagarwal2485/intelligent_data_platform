# Design: Silver and Gold performance, and the customer topic

## Context

See proposal.md for why. The measurements come from the `idp-silver` driver log
after the first stream on raw Bronze (default volume; driver plus 2 executors of
1 core and 1 GiB each). Total 3 min 54 s:

| Phase | Time | Detail |
|---|---|---|
| Pod, JVM and executors | 27 s | first job at +27 s |
| Reference conform + 4 dimensions | 51 s | 5 collects (~1–4 s each), driver-side Python conform, two commits per new table |
| `fact_usage` | 95 s | stage 77: conform, **19 s, 1 task**; stages 80/81: **16 s + 43 s, 200 tasks each** (merge) |
| `fact_recharge` | 17 s | 200-task stages, 9 s |
| `fact_fee_charge` | 31 s | 200-task stages, 12 + 6 + 5 + 3 s |
| Counts and shutdown | 9 s | |

87 stages took 191 s and ran 1,691 tasks. Each streaming query recorded
`spark.sql.shuffle.partitions -> 200` in its offset metadata.

**Gold** (`idp-gold` driver log, same run): 2 min 49 s in total. Adaptive execution
already coalesces its shuffles (36 stages, 105 tasks):

| Phase | Time | Detail |
|---|---|---|
| Pod, JVM, executors, Silver reads | ~32 s | |
| Reference times (`_ref_us`) | 17.6 s | 12 stages: a full-table `max()` + `collect` per aggregate, 5 in total (recharge and usage twice each, fees once) |
| `credit_features` write | 63 s | recomputes the recharge, usage and fee aggregates; the usage aggregation (two exact `countDistinct`s) is stage 24, 32.5 s; plus an empty create commit |
| `fraud_features` write | 21 s | recomputes the recharge and usage aggregates again (stage 40, 14.7 s) |
| Counts and shutdown | ~11 s | |

Relevant code:
- `transforms/session.py`: `spark_conf()` builds the session settings from the
  stage's profile (master, resources, `RUNTIME_CONF`). Off local, `builder_conf()`
  drops what spark-submit owns (`spark.master`, `spark.driver.*`,
  `spark.executor.*`); `spark.sql.*` isn't owned, so a session setting applies in the
  cluster too.
- `transforms/conform.py`: `conform_facts()` runs `mapInPandas` on the micro-batch
  as Spark partitioned it.
- `transforms/lake.py`: `ensure_table()` creates a table by appending an empty frame
  (one commit), and then the write commits again.
- `transforms/features.py`: `build_credit_features()` and `build_fraud_features()`
  each call `recharge_aggregates()` and `usage_aggregates()` (and `_ref_us()` inside
  them) on the Silver frames, with no caching; `pipeline.to_gold()` writes one after
  the other.
- `deploy/platform/kafka/create-topics.sh`: `customer` is `compact,delete` with
  `retention.ms: -1`.

## Goals / Non-Goals

**Goals:**
- `make silver` on the default minikube volume in about 2 minutes or less, with the
  same Silver output (the golden and Silver tests are unchanged).
- No hard-coded parallelism: everything derives from the stage profile, so a bigger
  profile scales without code changes (REQ-ORC-07).

**Non-Goals:**
- Faster per-event Python conforming (each event is validated against the event
  schema and then the resource schema). That's a later step if needed.
- Changing the executor sizing on minikube, which the node memory caps.

## Decisions

### 1. Shuffle partitions from the profile
- **The setting.** `spark_conf(cfg, stage)` sets `spark.sql.shuffle.partitions`:
  - the profile's `shuffle_partitions` if set;
  - else `2 × executor.instances × executor.cores` off local;
  - else `2 × N` for `local[N]` (and twice the CPU count for `local[*]`).

  Twice the cores leaves room for a little skew without hundreds of tiny tasks. For
  Silver on minikube that's 4.
- **Adaptive execution.** `RUNTIME_CONF` adds `spark.sql.adaptive.enabled=true` and
  `spark.sql.adaptive.coalescePartitions.enabled=true`. This is already Spark 3.5's
  default, but stating it guards against image or config drift. It helps the batch
  parts (dimensions, Gold, score); streaming micro-batches don't get it, which is why
  the explicit partition count matters most.
- **Checkpoints keep the old value.** Spark stores the partition count in each
  streaming checkpoint and reuses it, so existing checkpoints keep 200 until they're
  reset. The migration runs `make silver-full-refresh` once. A unit test checks that
  a fresh checkpoint records the profile's value.
- **Alternative rejected:** coalescing the micro-batch before `MERGE` with a fixed
  number. That would leave every other shuffle at 200 and hard-code a number.

### 2. Conform each fact batch on every core
`conform_facts()` repartitions the raw batch to the stage's total executor cores
(`spark.executor.instances × spark.executor.cores`, read from the session; for a
local session, the local cores) before `mapInPandas`. The batch is read once and the
round-robin repartition is cheap next to the Python work. The partition count is
taken from the session, so the profile alone decides it.

### 3. Create a new table in one commit
`lake.ensure_table()` creates a missing table from the frame's schema
(`writeTo(...).create()` on an empty projection, or the catalog's `create_table` with
the converted schema) instead of appending an empty frame. The first real write is
then the table's first snapshot: one commit per table on the first run instead of
two. Partitioning and properties are unchanged.

### 4. Gold: shared aggregates, computed once
`features.py` gains `shared_aggregates(silver, windows)`, which returns the
recharge, usage and fee aggregates for the run. `build_credit_features()` and
`build_fraud_features()` take them as an argument, and `to_gold()` builds them once,
`persist()`s them (they're per-subscriber, so small), writes both tables and then
unpersists.
- **Reference times in one pass.** Each table's reference time (`max` of its
  timestamp) is read in one small aggregate per table and passed in, rather than a
  `collect` per call: 3 scans instead of 5, outside the per-table rebuilds.
  Alternatively it can be a window over the aggregate; the explicit value keeps the
  logic as it is.
- **Unchanged output.** The functions keep their signatures for direct callers (the
  shared aggregates are optional parameters), so the feature values and the golden
  Gold tests stay identical.
- **Alternative rejected:** replacing the exact distinct counts with
  `approx_count_distinct`. It's faster, but it would change feature values and the
  models.

### 5. Customer topic: delete policy
`create-topics.sh` drops the `customer` special case: every resource topic gets
`cleanup.policy: delete` and the configured retention. Re-applying the CRs (`make
topics`) changes the existing topics in place through the Strimzi topic operator; no
messages are lost by switching from `compact,delete` to `delete` (retention starts
counting from now). The `purge-topics.sh` note about compacted topics goes. ADR-0031
gets a one-line note that compaction was withdrawn with raw Bronze (ADR-0032).

## Risks / Trade-offs

- **[Too few shuffle partitions for a large run]** → The default scales with the
  profile's cores, and `shuffle_partitions` overrides it. Adaptive execution still
  coalesces in batch jobs.
- **[Stale checkpoints keep 200 partitions]** → `make silver-full-refresh` in the
  migration. The Silver run log prints the partition count it uses, to make a stale
  checkpoint visible.
- **[Caching Gold aggregates costs executor memory]** → They're one row per
  subscriber (300 here, millions at most), far below the executors' heap. They're
  unpersisted after both writes.
- **[A repartition costs a shuffle of the raw batch]** → The raw rows are read once
  and shuffled once; the gain (2 or more cores instead of 1 for the Python UDF)
  outweighs it from small batches upwards. Measured in task 4.2.

## Migration Plan

1. `make image`, then `make spark-manifests` if the rendered manifests change.
2. `make topics`: the customer topics switch to the delete policy (check
   `kafka-topics --describe`).
3. `make silver-full-refresh`: rebuilds Silver and the checkpoints with the new
   partition count. Then the normal `make silver` cadence.

**Rollback:** revert. The shuffle setting goes with the code (after another full
refresh, the checkpoints use 200 again), and `make topics` with the old script
restores compaction.

## Results (task 5.2, minikube, default volume)

| Run | Before | After |
|---|---|---|
| `make silver` (incremental, after a stream of 75k events) | 3 min 54 s (driver); 87 stages, 1,691 tasks, 191 s of stages | **111 s** (driver 1 min 40 s); 86 stages, 150 tasks, 67 s of stages |
| `make gold` | 2 min 49 s; 36 stages, 146 s of stages | **63 s** (driver 55 s); 30 stages, 32 s of stages |
| `make silver-full-refresh` | n/a | 130 s; 83 stages, 125 tasks, at most 4 per stage |

Silver and Gold output is unchanged: 70,401 / 2,211 / 853 facts, 300 subscribers,
269 consenting subscribers in both feature tables, and no quarantined events. The
golden tests (local) pass unchanged. The customer topics report
`cleanup.policy=delete` and a message count (150 per tenant) in the Kafka UI.

Two test-only consequences of deriving parallelism from the session: the test
suite pins its Spark session to `local[2]` (`conftest.py`), because whichever test
creates the session first decides its cores. A `local[*]` session on a 12-core
laptop ran tiny test data on 24 partitions and 12 Python workers, and macOS ran out
of socket buffers. Production is unaffected: the cluster uses its profile, and a
local `idp run` uses the machine's cores.

