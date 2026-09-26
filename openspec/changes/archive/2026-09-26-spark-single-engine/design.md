# Design

## Context

- **Two implementations of the same transforms today:**
  - pandas: `src/idp/curation/silver.py` and `src/idp/features/{base,credit_features,fraud_features}.py`
  - PySpark: `pipelines/spark/{silver,gold}_job.py`

  The archived `spark-engine-parity` change proved them equivalent: matching schemas
  and row counts for all four Silver tables, and Gold values within ~1e-13. So the
  Spark code can become the single implementation without re-deriving any business
  rule.
- **The pandas path feeds everything except the Spark jobs:**
  - `idp.pipeline.to_silver/to_gold` accept in-memory frames.
  - `run()` passes data between stages in memory.
  - `train(dataset=..., silver=...)` has an in-memory feature path.
  - The shared test fixtures (`tests/conftest.py`: `silver`, `credit_labeled`,
    `fraud_labeled`) call `curate`/`build_*_features` directly.
- **The Spark jobs can't import `idp`.** They run in `apache/spark:3.5.3`, with the
  script mounted from ConfigMaps, and download `hadoop-aws` (~275 MB) on every run.
  The DAG picks pandas or Spark per stage from `idp_silver_engine`/`idp_gold_engine`.
- **The `idp` image** (`python:3.11-slim`) has no JVM.
- **CI:** GitHub Actions (`.github/workflows/ci.yml`) runs `pytest -q` on
  `ubuntu-latest` with Python 3.11, after `pip install -e ".[dev]"`. There is no
  JDK step, so the pyspark tests have never run in CI; they would be skipped anyway,
  because pyspark isn't in `[dev]`.

## Goals / Non-Goals

**Goals:**
- One transforms package, used by the CLI, the Kubernetes Jobs, Airflow and the tests.
- Stages exchange data only through the lake, locally and in the cluster.
- Explicit Spark driver memory, so single-pod Spark stages have predictable headroom.
- Keep today's Silver and Gold outputs, proved by golden-file tests.

**Non-Goals:**
- Iceberg, incremental processing, Bronze on Spark (change 2).
- The Spark Operator and executors, a separate Spark image, and scoring on Spark
  (change 3).
- MCP over Trino (change 4).
- Consent filtering. `consent-and-tenant-gateway` adds it after this change, in
  `idp.transforms`.

## Decisions

### 1. Promote the Spark job code, don't rewrite it
The code in `pipelines/spark/*_job.py` moves into `src/idp/transforms/` and gets
split up:

| Module | Contents |
|---|---|
| `session.py` | `get_spark(cfg)`: builds or reuses the SparkSession |
| `lake.py` | `layer_uri(cfg, layer)` giving `s3a://<prefix>-<layer>` or the local path; `read_table` / `write_table` (partition by `tenant_id`, per-tenant dynamic overwrite) |
| `tokens.py` | Spark salted-SHA-256 tokenization (`tok_` + sha256[:24]), plus `strip_uuid` |
| `silver.py` | Pure functions `build_dim_subscriber`, `build_fact_usage`, `build_fact_recharge`, `build_fact_fee_charge`, `curate(frames, salts)`, each DataFrame(s) → DataFrame |
| `features.py` | Pure functions for the recharge, usage and fee aggregates, `build_credit_features` and `build_fraud_features` |

The feature-name lists `CREDIT_FEATURES`/`FRAUD_FEATURES` move to
`src/idp/features/__init__.py`, the only thing left in that package. The models,
the API and the transforms all import them, so the copies in `gold_job.py` go away.

*Alternatives:*
- Keep pandas and port only its call sites: rejected, because the pandas
  implementation can't scale (see proposal.md).
- An engine-agnostic API (Ibis or Narwhals): rejected. It adds a second abstraction,
  its UDF and MERGE support is weaker, and change 2 needs Iceberg `MERGE` from Spark
  anyway.

### 2. I/O stays at the edges; transforms stay pure
Stage functions in `idp.pipeline` open the session, read the input layer, call the
pure transforms, write the output layer, and return only a summary (table names and
row counts).

- `to_silver(cfg)` and `to_gold(cfg)` drop their in-memory parameters.
- Row counts use Spark `count()`, and nothing is `collect()`ed or turned into pandas.
- The one exception is the tenure reference timestamp: a single scalar `agg(max)`,
  as today.

### 3. Session configuration in one place
`get_spark(cfg)` sets:

- **Master and memory:**
  - `spark.master = cfg.spark.master`: env `IDP_SPARK_MASTER`, default `local[*]`
  - `spark.driver.memory = cfg.spark.driver_memory`: env `IDP_SPARK_DRIVER_MEMORY`,
    default `1g`

  These must be set before the JVM starts, which is why they are builder settings.
- **Same behaviour as the parity-proven jobs:** UTC session timezone, dynamic
  partition overwrite, and `TIMESTAMP_MICROS` Parquet output.
- **When `cfg.s3.enabled`:** the s3a endpoint, credentials, path-style access and no
  SSL for MinIO. Locally, the `hadoop-aws` jars come from `spark.jars.packages` and
  are cached in `~/.ivy2`. In the image they are pre-installed, so nothing is
  downloaded at run time.
- **Reuse:** `getOrCreate`, so tests and `idp run` share one session per process.

`pyspark` is imported lazily inside `idp.transforms` and the Silver/Gold stage
functions. `idp serve`, `idp mcp`, `score`, `train` and `publish` never start a JVM.

### 4. One image for every stage
The `idp` image gains a headless JRE 17 and `pyspark==3.5.3` (moved from the
`[spark]` extra into the core dependencies). At build time it also downloads
`hadoop-aws-3.3.4` and `aws-java-sdk-bundle-1.12.262` into pyspark's `jars/`
directory.

The Silver and Gold Kubernetes Jobs and the Airflow tasks then run `idp silver` /
`idp gold` in the same image as every other stage. As a result:

- The `apache/spark` pods, the script ConfigMaps (`make spark-configmap`) and
  `deploy/app/jobs/spark-{silver,gold}-job.yaml` are removed.
- `pipelines/spark/{silver,gold}_job.py` become about 10-line entrypoints that call
  `idp.pipeline.to_silver/to_gold`, kept for `spark-submit` and the change-3
  `SparkApplication`.

*Alternative:* install `idp` into the `apache/spark` image. Rejected here because it
means two images to keep in lock-step. Change 3 revisits this with a baked Spark
image once executors are involved.

### 5. Airflow: no engine switch
In `idp_medallion_dag.py`:

- `_engine`, `_spark_stage`, `SPARK_SALTS`, `_silver_task` and `_gold_task` are
  deleted. Silver and Gold become plain `_stage("silver")` / `_stage("gold")`.
- Tenant salts come from the image's `config/platform.yaml`, the same source as
  every other stage, instead of the duplicated `--salts` string.
- The Silver and Gold pods get Spark-sized resources: request 1.5 Gi and limit 3 Gi,
  with `IDP_SPARK_DRIVER_MEMORY=1g`. That leaves headroom above the heap for
  off-heap and Python memory, which addresses the OOM seen in `spark-engine-parity`.

The Makefile targets `spark-configmap`, `spark-silver`, `spark-gold`,
`silver-engine` and `gold-engine` are removed.

**`idp_features` gets `max_active_runs=1`** (added during cluster verification).
Unpausing the DAG released a backlog of hourly runs alongside a manual run, and the
overlap caused two failures:
- A Gold run failed with `SparkFileNotFoundException`: another run's Silver had
  replaced `dim_subscriber` while it was reading it. Plain Parquet overwrite has no
  snapshot isolation; Iceberg fixes that in change 2.
- Two Silver pods were OOMKilled while several ran together. Each solo Silver run
  peaked at about 0.95 Gi.

Serializing feature refreshes removes both. Throughput isn't reduced, because the
refreshes are hourly and each takes a few minutes.

### 6. The local `run` goes through the lake
`run(cfg)`:
- generates the synthetic data
- `to_bronze`, which still writes Bronze with pandas
- `to_silver(cfg)`
- `to_labels(cfg, dataset)`
- `to_gold(cfg)`
- `train(cfg)`, on the lake path
- `to_scores(cfg)`

`--no-persist` keeps working: it points `cfg.storage` at a temporary lake directory
that is removed afterwards. In-memory composition no longer exists.

### 7. Tests: one Spark session and golden files
- **`tests/conftest.py`:**
  - A session-scoped `spark` fixture (`local[2]`, 4 shuffle partitions, UI off).
  - A session-scoped `lake` fixture that lands the existing multi-tenant synthetic
    dataset to Bronze and runs `to_silver` and `to_gold` into a temp lake once.
  - The existing `silver`, `credit_labeled` and `fraud_labeled` fixtures now return
    pandas frames read from that lake, so model and feature assertions stay in
    pandas.
- **Golden files:** before the pandas implementation is deleted, a one-off script
  writes its outputs for a small fixed dataset (20 subscribers, 2 tenants, 10 days,
  seed 3) to `tests/golden/{silver,gold}/*.parquet` (a few hundred KB).
  `tests/test_golden.py` checks the Spark stages against them, using the comparator
  from `test_spark_parity.py` (same columns and order, logical types, integers
  exact, reals within 1e-6). `test_spark_parity.py` is then removed.
- **SCN-ORC-05.3:** a test patches `pyspark.sql.DataFrame.toPandas` to raise, then
  runs `to_silver`/`to_gold`.
- **Tests no longer skip without pyspark.** A JDK is now required, and the README
  and `make install` note it.
- **CI:** `.github/workflows/ci.yml` gains an `actions/setup-java@v4` step
  (Temurin 17) before the install. pyspark is now a core dependency, so
  `pip install -e ".[dev]"` covers it.

## Risks / Trade-offs

- **The test suite gets slower**, from JVM start-up plus Spark jobs on tiny data;
  estimated ~2–3 min → ~4–6 min. → One shared session, one shared lake fixture, and
  small datasets in stage tests.
- **A JDK is now a prerequisite for development and CI.** → Documented,
  `make install` checks `java -version`, and CI installs Temurin 17. Temurin 11
  (local) and 17 (CI) are both supported by Spark 3.5.
- **The image grows by ~450 MB** (JRE, pyspark, AWS jars). → Accepted for one image
  per stage; change 3 can split it.
- **`dropDuplicates` keeps an arbitrary row among duplicates, where pandas kept the
  first.** → Bronze duplicates are identical re-landed records, so the output is the
  same. The golden test would catch any change.
- **`consent-and-tenant-gateway` targets the deleted pandas builders** (its tasks
  1.2 and 2.1). → After this change is archived, update that change so its filter
  goes into `idp.transforms.features` once.
- **Existing lakes are unaffected:** the storage layout, table names and schemas
  don't change, so no data migration is needed.

## Migration Plan

1. Merge, then `make image` (JRE, pyspark and jars baked in), then
   `make app-deploy`, then `make airflow-reload` (new DAG).
2. `kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow variables delete idp_silver_engine`
   (and the same for `idp_gold_engine`), then delete the ConfigMaps
   `idp-silver-spark` and `idp-gold-spark` in `idp-app`.
3. Verify with `make silver gold score publish` and `make timescale-verify`. Then
   trigger `idp_features` and confirm that `idp_inference` succeeds.
4. Rollback: the previous image and DAG (the lake layout is unchanged).
