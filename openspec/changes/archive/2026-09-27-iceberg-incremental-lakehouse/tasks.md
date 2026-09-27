# Tasks

## 1. Catalog foundation

- [x] 1.1 Spike: with `pyiceberg[sql-sqlite]` and Spark 3.5 + `iceberg-spark-runtime-3.5_2.12` + `sqlite-jdbc`, create a table in a SQLite JDBC catalog from Python, append from Spark, read from Python, and append from Python again. Verify with a throwaway test that both see every commit. Record the pinned versions in design.md (if interop fails, switch the local fallback to a Nessie container and update design.md before continuing)
- [x] 1.2 Add the `catalog` config section (`type: sql|rest`, `uri`, `warehouse`, `branch`) with `IDP_CATALOG_TYPE`/`IDP_CATALOG_URI` overrides, plus `lake.snapshot_retention_days`, `lake.bronze_archive_days` and `lake.target_file_size_mb`. Verify with config tests (SCN-FND-01.1, SCN-FND-01.5)
- [x] 1.3 Create `src/idp/common/catalog.py`: `catalog_config(cfg)`, `load_catalog(cfg)`, `ensure_namespaces`, `read_table(cfg, layer, table, row_filter=None) -> pandas`, `append_table`, and `create_or_evolve` (union-by-name schema evolution; table location = `lake_uri(layer)/<table>`). Verify with unit tests on the SQLite catalog: create, evolve, filtered read (SCN-FND-04.1, 04.2, 04.4)
- [x] 1.4 Configure the Spark `lake` catalog in `transforms/session.py` from `catalog_config(cfg)` (Iceberg SQL extensions, jars via packages locally), and rewrite `transforms/lake.py` for Iceberg: `read_table`, `overwrite_tenants` (`overwritePartitions`), `merge_insert_new(table, df, keys)`. Verify with a test that Spark and pyiceberg see each other's commits (SCN-FND-04.3)

## 2. Bronze on Iceberg

- [x] 2.1 Rewrite `to_bronze` to append through `catalog.append_table` into `bronze.<entity>` with `_ingested_at`/`ingest_date`, partitioned by `(tenant_id, ingest_date)`. Verify with tests for SCN-BRZ-01.1, 01.2 (partition spec) and 01.3 (two batches become two snapshots, nothing rewritten)
- [x] 2.2 Make `idp consume` commit Kafka offsets only after the Bronze append succeeds (disable auto-commit, commit after `to_bronze`). Verify with a unit test using a fake consumer that fails the append: offsets not committed. Also cover SCN-STR-02.1 and 02.2 (no snapshot when empty)

## 3. Incremental Silver

- [x] 3.1 Rebuild `dim_subscriber` from the Iceberg reference tables with `overwrite_tenants`. Verify that SCN-SLV-06.1/06.2 pass on Iceberg
- [x] 3.2 Implement the incremental facts: `readStream` from the Iceberg Bronze sources, `trigger(once=True)` (see design §4 on why not `AvailableNow`), per-fact/source checkpoints under `<silver>/_checkpoints/`, and `foreachBatch` → `build_fact_*` → batch dedup → `MERGE ... WHEN NOT MATCHED THEN INSERT *` (merge-on-read on the facts). Verify with tests for SCN-SLV-07.1 (only the new batch read), 07.2 (no new snapshot when nothing new) and 07.4 (fail before commit, rerun, no loss or duplicate)
- [x] 3.3 Add `idp silver --full-refresh` (drop checkpoints, replace facts, rerun from the first snapshot). Verify with a test for SCN-SLV-08.1
- [x] 3.4 Equivalence test: land Bronze in 3 batches with Silver after each, compare with a one-shot full build of the same Bronze (SCN-SLV-07.3). Also run the golden tests against the incremental path
- [x] 3.5 Concurrency test: a reader scans a Silver table in a loop while Silver commits; it never raises and always sees a complete snapshot (SCN-SLV-06.3)

## 4. Gold, scores and Python readers

- [x] 4.1 Gold reads Iceberg Silver and writes features with `overwritePartitions`. Verify that `test_golden.py`, `test_stages_lake.py`, `test_idempotent_writes.py` and `test_consent.py` pass unchanged in intent (SCN-GLD-04.1/04.2)
- [x] 4.2 Switch Score (append), train, publish and `_read_labels`-adjacent readers to `catalog.read_table`, keeping labels as Parquet in `idp-ml`. Verify that the score, publish and training tests pass
- [x] 4.3 MCP tools: read Silver and Gold through `catalog.read_table` with a `tenant_id` row filter, still passing results through `scope_to_tenant`. Unreachable catalog → `LakeReadError`; untagged table → isolation error. Verify that `test_mcp_tools.py` is green

## 5. Maintenance, archiving, migration

- [x] 5.1 Implement `idp maintain` (`rewrite_data_files`, `expire_snapshots` bounded by the oldest checkpoint offset, `remove_orphan_files`) over all lake tables. Verify with tests for SCN-ORC-06.1 (same rows, fewer files), 06.2 (expired snapshots, and the next incremental run still processes only new Bronze, including after compaction) and 06.3 (Bronze rows intact)
- [x] 5.2 Implement `idp migrate-lake` (Parquet Bronze → Iceberg Bronze, with a migration marker property). Verify with a test that builds a Parquet lake with the pre-change code path (fixture), migrates it, runs `silver --full-refresh` + `gold`, and gets equal Gold
- [x] 5.3 Render the Bronze archive rule (MinIO ILM JSON / S3 lifecycle) from `lake.bronze_archive_days` with a `make lake-archive-policy` target. Verify the rendering with a test, and on MinIO confirm the rule parses; MinIO rejects it only for the missing tier ("Invalid storage class"), so without a remote tier the target renders only (documented)

## 6. Deploy

- [x] 6.1 Lake catalog in the cluster: the `iceberg` database on TimescaleDB (idempotent `deploy/platform/timescaledb/lake-catalog.sh`, run by `install-infra.sh` and `make lake-catalog`), used as an Iceberg JDBC catalog with the Postgres driver. Verify on minikube with SCN-PLT-09.1–09.3. *(Revised during 7.2: the first implementation used Nessie REST with a JDBC2 store. It passed the restart and REST probes, but it keeps only a table's current snapshot, which breaks incremental Silver; see design §1.)*
- [x] 6.2 Dockerfile: bake `iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle`, `sqlite-jdbc` and `postgresql` next to the S3A jars, and add `pyiceberg` to requirements. Verify that `make image` succeeds and `get_spark` in the image loads the `lake` catalog with no downloads
- [x] 6.3 Catalog env (`IDP_CATALOG_TYPE=sql`, `IDP_CATALOG_URI` = the `iceberg` Postgres database) on every stage Job, the scoring-api Deployment and the DAG pod env. Add the `idp_maintenance` DAG and a `deploy/app/jobs/maintain-job.yaml` + `make maintain`. Verify DAG import with the stub check and in Airflow (four DAGs, SCN-ORC-02.5)
- [x] 6.4 Update `README.md`, `deploy/README.md` (migration steps, archive policy caveat) and add an ADR to `docs/decisions/decision-log.md` (Iceberg lake on a JDBC catalog; incremental Silver facts; Gold full recompute), updating ADR-0002's status. Verify by reading

## 7. Verification

- [x] 7.1 `pytest -q` is green (runtime recorded), including the golden tests — 204 passed in 7m31s (2026-09-27, re-run after the catalog change)
- [x] 7.2 On minikube, run the migration plan end to end: migrate, full refresh, gold, score, publish. Then `make stream` for a second batch and `make silver`, and verify with Spark `snapshots` metadata that only the new batch was read and that Silver/Gold/serving counts are correct and consent-filtered. Trigger `idp_features` → `idp_inference` → success, then run `make maintain` and verify that the counts are unchanged — Done 2026-09-27 on the JDBC catalog. migrate-lake matched the legacy Bronze exactly. The full refresh gave Silver 300/70,401/2,211/853 and Gold 269, equal to the pre-migration Parquet. Consent: 269 of 300 consenting, and Gold and every serving table hold only consenting keys. Second batch via `make stream`: one new Bronze snapshot per entity, and `make silver` processed exactly that batch (70,401 usage rows, not the 140,802 in Bronze) with Silver unchanged (MERGE de-duplicated); a re-run processed 0. `idp_features` (scheduled + manual) and the dataset-triggered `idp_inference` runs succeeded. `make maintain` compacted files (e.g. fact_usage 56→28) with row counts and consent unchanged, and Silver afterwards processed 0. Issues found and fixed on the way: Nessie REST keeps only the current snapshot (catalog moved to JDBC/Postgres); migration now commits one snapshot per entity; `fs.s3.impl` for orphan cleanup; minikube node container memory cap (raised to 12 GB, documented).
