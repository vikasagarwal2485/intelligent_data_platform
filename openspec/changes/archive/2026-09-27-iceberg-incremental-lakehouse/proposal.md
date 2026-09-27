# Proposal

## Why

The lake is plain Parquet, and every Silver run rebuilds every table from **all** of
Bronze. The cost grows with history rather than with new data. For an operator with
~250 M usage events a day, re-reading months of Bronze every hour is not viable.

Plain Parquet also has no snapshot isolation:
- A Gold run failed with `No such file or directory` while another run rewrote
  `dim_subscriber` underneath it.
- `max_active_runs=1` only works around this for Airflow; ad-hoc runs can still
  collide.

Iceberg and the Nessie catalog were chosen for this in ADR-0002 and ADR-0011, and
are deployed, but they aren't used. Nessie still runs an in-memory version store, so
a restart would lose every table definition.

This is change 2 of the lakehouse roadmap. It builds on `spark-single-engine`, which
is archived.

## What Changes

- **Iceberg tables in a persistent JDBC catalog.** Bronze, Silver and Gold become
  Iceberg tables in namespaces `bronze`, `silver` and `gold`. Each table keeps its
  data in its layer's bucket (`s3://idp-<layer>/tables/<table>`). An Iceberg JDBC
  catalog is the single source of table state: the `iceberg` database on the
  in-cluster TimescaleDB/Postgres, and SQLite locally. Readers see committed
  snapshots, so a writer can no longer pull files out from under them.
  - *Revised during apply:* the plan was Nessie's Iceberg REST catalog. It keeps
    only a table's current snapshot, which breaks the incremental Silver
    checkpoints, so Nessie stays as deployed but is not on the lake path (design §1,
    ADR-0028).
- **Bronze is partitioned by `(tenant_id, ingest_date)`.** `idp consume` and the local
  landing stay a Python process, as you decided, but append to Iceberg Bronze with
  `pyiceberg`. Each record gets the landing time: `_ingested_at` and `ingest_date`.
  Bronze remains append-only and immutable. Nothing is deleted or moved to avoid
  reprocessing.
- **Incremental Silver.**
  - The **event facts** (`fact_usage`, `fact_recharge`, `fact_fee_charge`) process
    only the Bronze snapshots appended since the last run. This uses Spark Structured
    Streaming over the Iceberg Bronze tables with a run-to-end trigger and a
    checkpoint, and each micro-batch is `MERGE`d into Silver on `(tenant_id, id)`.
    Replaying the same Bronze never duplicates.
  - The **reference dimension** `dim_subscriber` (parties, accounts, agreements,
    products) is small, one row per subscriber, and is still rebuilt in full each run.
  - `idp silver --full-refresh` resets the checkpoints and rebuilds the facts from all
    of Bronze. That's the reprocessing path after a logic fix, and the reason Bronze
    stays immutable.
- **Gold stays a full recompute**, as you decided, now from Iceberg Silver. Feature
  values are unchanged, and the golden tests must still pass. Score tables stay
  append-only history as Iceberg appends.
- **pandas readers use Iceberg with pushed-down filters.** Train, Score, publish and
  the MCP tools read through `pyiceberg` with a `tenant_id` filter where one applies,
  instead of reading whole tables.
- **Lake maintenance.** A new stage, `idp maintain`, and a daily `idp_maintenance`
  DAG compact small files (frequent consumes create many), expire old snapshots and
  remove orphan files. Bronze also gets an archive policy: it moves to a colder
  storage class after a configurable age (S3 lifecycle on AWS). This controls cost and
  is not a way to avoid reprocessing.
- **Local and CI use the same Iceberg code** on a file-based catalog: a SQLite JDBC
  catalog that both `pyiceberg` and Spark read, with a local warehouse directory. No
  Nessie or MinIO is needed.
- **Migration:** a one-off `idp migrate-lake` imports the existing Parquet Bronze into
  Iceberg. Silver and Gold are then rebuilt with `--full-refresh`.

Non-goals:
- A daily-aggregate Gold. You decided on a full recompute; it's a possible later
  change.
- Streaming Bronze ingestion with Spark (the consumer stays Python).
- Distributed Spark executors (change 3, `distributed-spark-runtime`).
- Trino for the MCP tools (change 4, `mcp-over-trino`).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `bronze-ingestion`: REQ-BRZ-01 (Iceberg Bronze partitioned by
  `(tenant_id, ingest_date)`, append-only).
- `streaming-kafka`: REQ-STR-02 (the consumer appends to Iceberg Bronze).
- `silver-curation`: REQ-SLV-04 (reads Iceberg Bronze) and REQ-SLV-06 (idempotent via
  checkpoint plus `MERGE`); new REQ-SLV-07 (incremental event facts) and REQ-SLV-08
  (full refresh).
- `gold-features`: REQ-GLD-04 (tenant partitions replaced atomically in Iceberg).
- `foundation`: REQ-FND-01 (catalog configuration and env) and REQ-FND-04 (lake
  tables resolved through the Iceberg catalog; filtered reads).
- `orchestration`: REQ-ORC-01 (`idp maintain`) and REQ-ORC-02 (fourth DAG,
  `idp_maintenance`); new REQ-ORC-06 (lake maintenance).
- `platform-deployment`: new REQ-PLT-09 (persistent Iceberg JDBC catalog on the
  platform Postgres).

## Impact

- **Code:**
  - new `src/idp/common/catalog.py`: catalog config, `pyiceberg` read, append and
    replace
  - `src/idp/transforms/{session,lake}.py`: Iceberg Spark catalog and
    `MERGE`/`overwritePartitions`
  - `src/idp/transforms/silver.py` callers: incremental facts
  - `src/idp/pipeline.py` (all stages)
  - `src/idp/mcp/tools.py`: Iceberg reads with a tenant filter
  - new `src/idp/maintenance.py`
  - `src/idp/cli.py`: `maintain`, `migrate-lake`, `silver --full-refresh`
- **Dependencies:**
  - Python: `pyiceberg[s3fs,sql-sqlite,sql-postgres]`
  - Spark jars baked into the image: `iceberg-spark-runtime-3.5_2.12`,
    `iceberg-aws-bundle`, `sqlite-jdbc`, `postgresql`
- **Deploy:** the `iceberg` database on TimescaleDB (`make lake-catalog`), an
  Airflow maintenance DAG, and catalog env on every stage Job and pod.
- **Data:** a one-off migration. Table names and columns are unchanged, plus
  `_ingested_at` and `ingest_date` in Bronze. Labels stay Parquet in `idp-ml`, which
  isn't part of the lake.
- **Tests:** run on the local SQLite catalog. The golden tests must pass unchanged.
- **Later changes:** `distributed-spark-runtime` and `mcp-over-trino` build on this.
  In-flight `mlflow-model-registry` is unaffected: models stay outside the lake.
