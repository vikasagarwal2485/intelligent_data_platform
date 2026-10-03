# Tasks

## 1. Shuffle parallelism

- [x] 1.1 Make `spark_conf()` set `spark.sql.shuffle.partitions` from the stage profile (design §1): the profile's `shuffle_partitions`, else `2 × instances × cores` off local, else `2 × local cores`. Add the adaptive-execution settings to `RUNTIME_CONF`, and accept `shuffle_partitions` in the profiles. Verify with unit tests:
  - SCN-ORC-07.5: a 2×1-core profile gives 4;
  - SCN-ORC-07.6: an explicit 16 gives 16;
  - `local[2]` gives 4;
  - `builder_conf()` keeps the setting when spark-submit launched the process.
- [x] 1.2 Verify that streaming uses it: a Silver run in a fresh lake records the profile's partition count in the fact checkpoints' offset metadata, and the fact merges' shuffle stages run that many tasks (read from the Spark listener or the offsets file in a test).

## 2. Conform in parallel, create in one commit

- [x] 2.1 Repartition each raw fact batch to the session's total executor cores before `mapInPandas` in `conform_facts()` (design §2). Verify with a test on a multi-partition local session that the conformed output is unchanged (SCN-SLV-09.1 still passes) and that the conform stage runs one task per core.
- [x] 2.2 Make `lake.ensure_table()` create a missing table from the schema without appending an empty snapshot (design §3). Verify that the first write to a new Silver table leaves exactly one snapshot, and that the Silver, incremental and golden tests pass unchanged.

## 3. Gold

- [x] 3.1 Build the shared Gold aggregates once (design §4): `shared_aggregates()` with each table's reference time read once, used by both feature builders and persisted in `to_gold()` for the two writes. Verify:
  - the golden Gold tests pass unchanged;
  - `test_transforms_features.py` passes;
  - a test that `to_gold()` runs each aggregation once: counted Spark jobs, or a spy on `usage_aggregates`.

## 4. Customer topic

- [x] 4.1 Give `t.<tenant>.customer` the delete policy and the standard retention in `create-topics.sh`, and drop the compaction notes from `purge-topics.sh`. Verify by rendering the CRs (the stub-kubectl dry run used before) and a server-side dry run: all resource topics share one policy (SCN-STR-01.9).

## 5. Docs and verification

- [x] 5.1 Update the docs:
  - the README's Kafka topics table and DLQ notes (no compaction), and Silver and Gold performance notes: profile-driven shuffle parallelism, the full refresh after changing it, and the shared Gold aggregates;
  - `deploy/README.md`;
  - `deploy/platform/kafka/README.md`;
  - `config/platform.yaml` (the `shuffle_partitions` option in the profiles comment);
  - a note on ADR-0031 in the decision log.

  Verify by reading.
- [x] 5.2 Verify:
  - `pytest -q` is green;
  - on minikube: `make image`, `make spark-manifests` if they change, and `make topics`. The customer topics show `cleanup.policy=delete` and a message count in the Kafka UI;
  - `make silver-full-refresh`, then `make stream-from-landing`, `make silver` and `make gold`: record each run's time and its stage and task counts from the driver logs. The targets at the default volume are about 2 minutes or less for `make silver` and about 1.5 minutes or less for `make gold`;
  - Silver's row counts equal the previous run's (70,401 / 2,211 / 853 facts; 300 subscribers), the quarantine is empty, and Gold has the same subscribers (269 consenting) with feature values equal to the previous run's.

  Ask the user before any reset; keep the DAGs paused.
