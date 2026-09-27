# Design

## Context

- **Lake I/O today goes through three paths:**
  - `common/storage.py`: pandas Parquet, used by the Bronze landing (`to_bronze`),
    Score/train/publish reads (`_read_layer`), labels, and the MCP tools
    (`_read_layer_table` reads a whole table, then filters by tenant through
    `scope_to_tenant`).
  - `transforms/lake.py`: Spark Parquet with dynamic partition overwrite, used by
    Silver and Gold.
  - Every Silver run reads all of Bronze.
- **Nessie** is chart 0.108.1 with `versionStoreType: IN_MEMORY`. **Trino** 480 has
  no catalog configured. **TimescaleDB** already hosts the Airflow metadata database.
- **The Spark runtime** is `get_spark(cfg)` in `local[*]` inside the `idp` image,
  which bundles a JRE, pyspark 3.5.3 and the S3A jars.
- **Guards that must keep passing:** the golden tests (`tests/golden/`) and the
  consent, idempotency and stage-composition tests.

## Goals / Non-Goals

**Goals:**
- One catalog (Iceberg JDBC: Postgres in the cluster, SQLite locally) behind a single
  config section, used by Spark and Python alike.
- Snapshot-isolated reads and atomic commits for every table.
- Silver facts cost O(new Bronze) per run and stay exactly equal to a full rebuild.
- An operationally complete lake: maintenance, archiving and migration.

**Non-Goals:**
- Daily-aggregate Gold (Gold stays a full recompute) and streaming Bronze ingestion
  in Spark. Both follow from your decisions.
- Executors and the Spark Operator (change 3). MCP over Trino (change 4).
- Slowly-changing-dimension history for `dim_subscriber`. It stays the current state
  (SCD1), rebuilt each run.
- Row-level corrections to already-landed events. Facts are insert-if-absent.

## Decisions

### 1. Catalog: Iceberg JDBC everywhere, on Postgres in the cluster and SQLite locally
- **Cluster:** `IDP_CATALOG_TYPE=sql` with `IDP_CATALOG_URI` set to
  `postgresql+psycopg2://…@timescaledb.idp-platform.svc:5432/iceberg`, which is the
  `iceberg` database on the platform TimescaleDB, created by
  `deploy/platform/timescaledb/lake-catalog.sh` / `make lake-catalog`. `pyiceberg`
  (SqlCatalog) and Spark (JdbcCatalog plus the Postgres JDBC driver) use the same
  catalog tables as the local path. Both engines reach S3 with their own
  credentials through S3FileIO.
- **Why not Nessie (the original plan).** The end-to-end run (task 7.2) showed three
  problems:
  - Nessie's Iceberg REST catalog keeps only a table's **current** snapshot in its
    metadata, and tracks history as Nessie commits. The incremental Silver
    checkpoints reference Bronze snapshots by id, so after the next append the
    stream can't resume ("Cannot load current offset"). Snapshot expiry has nothing
    to expire, and orphan cleanup could delete files that older Nessie commits
    reference. Nessie GC would be needed instead.
  - Nessie also ignores table locations passed by the client, and by default it
    signs S3 requests only under its warehouse location.
  - Proven on minikube: three separate appends left one snapshot.

  Nessie stays deployed as before (ADR-0011) but is not on the lake path. ADR-0028
  records the decision.
- **Local, tests and CI:** the Iceberg **JDBC catalog on SQLite**
  (`<storage.root>/catalog.db`). `pyiceberg`'s `SqlCatalog` and Iceberg-Java's
  `JdbcCatalog` share the same schema (`iceberg_tables`,
  `iceberg_namespace_properties`), so the Python landing and Spark see the same
  tables with no service running.
- **Spike results (task 1.1)** with pinned versions `pyiceberg==0.12.0`,
  `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0` and
  `org.xerial:sqlite-jdbc:3.46.1.3`. `pyiceberg` and Spark share one SQLite JDBC
  catalog both ways: append, `INSERT`, `MERGE`, and an `AvailableNow` stream that on
  its next run reads only the newly appended snapshot. Three settings are required:
  - SQLite in **WAL** mode, with `connect_args.timeout` on the `pyiceberg` side.
    Otherwise a `pyiceberg` commit after Spark has connected fails with
    `database is locked`.
  - `spark.sql.catalog.lake.jdbc.init-catalog-tables=false`. The JDBC catalog's
    start-up table-existence check leaves a SQLite read transaction open, so the
    Spark session never sees later external commits. The `pyiceberg`
    `SqlCatalog` creates the catalog tables instead, and is always initialized
    first.
  - `spark.sql.catalog.lake.cache-enabled=false` everywhere, so every stage (a
    separate process in the cluster) reads the latest committed snapshot.
- `common/catalog.py` exposes `catalog_config(cfg)`, which both `load_catalog`
  (pyiceberg) and `get_spark` (Spark `spark.sql.catalog.lake.*`) use. The two can't
  drift.
- *Alternatives:*
  - Nessie REST, the original plan. Rejected after task 7.2 (see above). Keeping it
    would need a redesign: a high-water mark on `_ingested_at`, with a gap risk
    under concurrent consumers, plus Nessie GC.
  - Nessie's native Spark catalog (`NessieCatalog`) has no `pyiceberg` equivalent, so
    it would force two code paths. Rejected.
  - A Hadoop (file-based) catalog doesn't support `pyiceberg` writes. Rejected.

### 2. Table layout
- **Namespaces** are `bronze`, `silver` and `gold`. Each table is created with
  `location = <lake_uri(layer)>/tables/<table>`, so data stays in the layer's
  bucket and bucket-level policies (lifecycle, access) still work per layer.
  - Each layer namespace also carries a `location` property of
    `<lake_uri(layer)>/tables`, set by `catalog.ensure_namespaces`. The JDBC catalog
    honors the explicit table location. The namespace location is the default for
    catalogs that don't: Nessie REST, for example, places a new table at
    `<namespace location>/<table>_<uuid>`.
  - The `tables/` prefix keeps Iceberg data apart from the legacy Parquet at
    `<lake_uri(layer)>/<table>`. Otherwise orphan-file cleanup would treat the
    legacy files, including the migration source, as orphans and delete them.
- **Catalog name** is `lake` for REST. For a SQL catalog it is
  `lake_<sha1(uri)[:10]>`: the name is stored in every SQL-catalog row, and both
  engines must use the same one. Catalogs are registered on the Spark session at run
  time (`transforms.lake.attach`), so one session (for example the test session)
  serves several local lakes.
- **Bronze** `bronze.<entity>` is partitioned by `identity(tenant_id), identity(ingest_date)`.
  Columns: the landed record plus `_ingested_at` (timestamp UTC) and `ingest_date`.
- **Silver facts** are partitioned by `identity(tenant_id), days(<event_ts>)`, where
  the event timestamp is `started_at`, `created_at` or `charged_at`. That prunes
  both the per-tenant MCP reads and the windowed feature scans. `dim_subscriber` is
  partitioned by `tenant_id`.
- **Gold** features are partitioned by `tenant_id`. Score tables by `tenant_id` and
  `days(scored_at)`.
- Labels stay Parquet in `idp-ml`. They're an ML artifact, kept deliberately outside
  the lake (REQ-ML-06).

### 3. Bronze landing: Python append through pyiceberg
`to_bronze` (used by `idp consume` and `run`):
1. Adds `_ingested_at`/`ingest_date` to each entity frame.
2. Converts it to Arrow.
3. Creates the table on first write, or evolves the schema with
   `update_schema().union_by_name()` when a batch brings new columns. Columns are
   never dropped.
4. Calls `table.append()`, which is one snapshot per entity per batch.

Consuming a batch and landing it are two steps, so a crash between them could
re-land records after a replay. Kafka offsets are committed only after the append
succeeds. Duplicated Bronze records are harmless, because Silver merges on the
natural key (REQ-SLV-06).

### 4. Incremental Silver facts: Structured Streaming over Iceberg with a run-to-end trigger
For each fact, `spark.readStream.format("iceberg")` reads its Bronze source(s):
- `fact_usage` reads `voice_events` and `data_events`.
- `fact_recharge` reads `recharge_events`.
- `fact_fee_charge` reads `fee_charges`, and also reads `products` as a batch lookup.

The streams run with `trigger(once=True)` and a checkpoint at
`<lake_uri(silver)>/_checkpoints/<fact>/<source>`. Each run processes all
available Bronze in one micro-batch, because no read limit is set.

`Trigger.AvailableNow` was rejected after the maintenance tests. On every restart,
Iceberg 1.10's `prepareForTriggerAvailableNow` re-validates the stream's *initial*
offset (the first Bronze snapshot, persisted under `sources/0/`). Once snapshot
expiry removes that snapshot, the stream fails with "Cannot load current offset".
The alternative, keeping every stream's first snapshot forever, would block Bronze
expiry indefinitely. `Trigger.Once` resumes only from the last committed offset,
which maintenance protects (decision 6). `Trigger.Once` is deprecated in Spark 3.5
but still supported; revisit this if a later Iceberg release stops checking the
initial offset.

`foreachBatch`:
1. applies the same pure transform (`build_fact_*`)
2. de-duplicates the batch on `(tenant_id, id)`
3. runs `MERGE INTO silver.<fact> t USING batch s ON t.tenant_id = s.tenant_id AND t.id = s.id WHEN NOT MATCHED THEN INSERT *`

This gives exactly-once semantics: the checkpoint records progress only after the
batch commits, and the insert-if-absent `MERGE` makes replays harmless.
- **`dim_subscriber`** and its product counts are rebuilt in full from the four
  reference tables with `overwritePartitions()`. They're one row per subscriber, so
  the cost is bounded by subscriber count, not event volume. The tenure reference
  time stays batch-global, as the golden files expect.
- **`--full-refresh`** deletes the fact checkpoints and truncates the facts in one
  replace commit, then runs the same streams from the first Bronze snapshot.
- *Alternatives:*
  - A hand-rolled "last processed snapshot id" stored as a table property. Rejected:
    it's not atomic with the `MERGE` commit and duplicates what Structured Streaming
    already provides.
  - Nessie branch-per-run with a merge to `main`. Deferred; it's more powerful but
    not needed for exactly-once here.

### 5. Gold and scores
- **Gold** reads the Iceberg Silver snapshots and writes the feature tables with
  `writeTo(...).overwritePartitions()` (atomic per tenant).
- **Score** appends through `pyiceberg` (`table.append`), and publish, train and Score
  read through `pyiceberg` scans. Score keeps its pandas implementation until
  change 3.
- `_read_layer` becomes `catalog.read_table(cfg, layer, table, row_filter=None)`,
  which returns pandas.
- The MCP tools scan with `EqualTo("tenant_id", t)`, and results still pass through
  `scope_to_tenant` (REQ-FND-06).

### 6. Maintenance and archiving
`idp maintain` runs Iceberg Spark procedures per table:
- `rewrite_data_files` (binpack, a target file size in config)
- `expire_snapshots` (`older_than = now - retention`, `retain_last = 1`). For a
  Bronze table, the cutoff is capped at the oldest snapshot referenced by the latest
  offset of any Silver checkpoint, so the next incremental run can always resume.
  The cutoff is a UTC-qualified timestamp literal, because an unqualified
  `TIMESTAMP '...'` is read in the session time zone.
- `remove_orphan_files` (`older_than` is at least 24 hours ago, Iceberg's guard
  against deleting a concurrent writer's uncommitted files)

Bronze archiving is object-store configuration, not code:
- A MinIO ILM or S3 lifecycle rule on `idp-bronze` transitions objects older than
  `lake.bronze_archive_days` (default 90) to a colder tier or storage class. On EKS
  that's S3 Glacier Instant Retrieval, which keeps objects readable, so reprocessing
  still works.
- On minikube, MinIO has no remote tier, and MinIO rejects a transition rule that
  names an unknown tier ("Invalid storage class"). So `make lake-archive-policy`
  only renders the rule there. With `ARCHIVE_TIER=<tier>` set, it imports the rule
  once a tier is configured (`mc ilm tier add`). On EKS, the S3 form
  (`idp archive-policy --format s3`) is applied with
  `aws s3api put-bucket-lifecycle-configuration`.
- The rule is scoped to the `tables/` prefix, which holds the Iceberg data and
  metadata. Legacy Parquet, kept only until the operator removes it, is not
  archived.

### 7. Runtime packaging
- **The image bundles** `iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle` (for
  S3FileIO against MinIO) and `sqlite-jdbc`, next to the S3A jars, again with no
  downloads at run time.
- **Local and tests** use `spark.jars.packages` (cached in `~/.ivy2`), as the S3A jars
  already do.
- **Versions** are pinned in one place (config and Dockerfile): Iceberg 1.6.x for
  Spark 3.5, and a compatible `pyiceberg` release.

### 8. Migration of the existing lake
`idp migrate-lake`:
1. Reads each existing Parquet Bronze entity.
2. Appends it to `bronze.<entity>` with `_ingested_at` set to the migration time.
3. Records the migration in the table properties.

Then `idp silver --full-refresh` and `idp gold` rebuild Silver and Gold, and
`idp score` writes a first Iceberg score run. The old Parquet score history is not
migrated: the MCP score windows restart from the migration time. This is documented.
The old Parquet paths are left in place until the operator removes them, once the
Iceberg lake checks out.

## Risks / Trade-offs

- **pyiceberg `SqlCatalog` and Iceberg-Java `JdbcCatalog` interoperability on
  SQLite.** Both follow the same schema, but versions can drift. → Task 1.1 is a
  spike: write with one, read with the other, commit from both. If they don't
  interoperate, the local fallback becomes a local Nessie container
  (`docker compose`), and the tests take a service dependency.
- **The Iceberg streaming source reads append snapshots only.** Bronze is append-only
  by requirement, and maintenance's compaction creates replace snapshots, which the
  source skips (`streaming-skip-overwrite-snapshots=true`). → Covered by a test that
  runs compaction between two incremental runs.
- **Snapshot expiry vs checkpoints.** If a snapshot that a checkpoint still points to
  expires, the stream fails. → Maintenance expires only snapshots older than the
  oldest checkpoint offset, and the retention default (7 days) is far longer than the
  hourly cadence.
- **Many small files** from hourly `MERGE`s and frequent consumes. → Daily
  compaction. Files are sized in config.
- **The first `MERGE` on a large fact** is a full-join rewrite. → Merge-on-read
  (`write.merge.mode=merge-on-read`) on the facts keeps `MERGE` cost proportional to
  the batch. Compaction folds the delete files in.
- **Golden tests must stay exact.** Iceberg changes only the storage, but row order
  and the types of partition columns can differ. → The golden comparator already
  sorts by key and compares logical types. Any difference is a bug, not a golden
  update.
- **The kind node's memory cap vs what Kubernetes sees.** In the Docker driver, the
  minikube node container was capped at 6 GB while Kubernetes saw 21 GB. So Spark
  pods were killed by the node-level OOM killer well below their own limits. → The
  deploy README documents raising the cap
  (`docker update --memory 12g --memory-swap 12g minikube`).
- **Operational weight:** one more database on the platform Postgres (`iceberg`) and one more DAG. → Covered by
  REQ-PLT-09 and the deploy README.

## Migration Plan

1. Create the catalog database (`make lake-catalog`), then `make image` (with the
   Iceberg jars), then `make app-deploy`, then `make airflow-reload`.
2. `idp migrate-lake`, then `idp silver --full-refresh`, then `idp gold`, then
   `idp score`, then `idp publish`.
3. Verify:
   - the Silver and Gold row counts match the pre-migration Parquet (the lakecheck
     script)
   - consent-filtered serving is still correct
   - `idp_features` and `idp_inference` still succeed
4. Rollback: the previous image reads the untouched Parquet paths. The `iceberg`
   database can be dropped.
