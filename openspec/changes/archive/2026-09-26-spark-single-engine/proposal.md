# Proposal

## Why

The Silver and Gold business logic exists twice. The pandas version
(`src/idp/curation/silver.py`, `src/idp/features/*`) backs the `idp` CLI stages,
the local `run` and the tests. The PySpark version
(`pipelines/spark/{silver,gold}_job.py`) is the production engine. Only a parity
test (REQ-ORC-04) keeps them in step, so every rule change is written, reviewed
and tested twice.

The pandas path also cannot run at telecom scale:

- It loads whole tables into one pod's memory.
- A mid-size operator (5 M subscribers, ~250 M usage events/day) needs ~22 B events
  for a 90-day feature window.
- A Tier-1 operator (50 M subscribers) needs over 200 B.

ADR-0010 chose pandas so the prototype could be tested without a cluster. That
trade-off no longer holds: the Spark jobs already run locally and in CI, as the
parity tests show.

This is the first of four changes that take the lakehouse to operator scale (see
Roadmap).

## What Changes

- **One implementation of the Silver and Gold transforms, in PySpark.** A new
  package `src/idp/transforms/` holds pure DataFrame functions:
  - curation: `dim_subscriber`, `fact_usage`, `fact_recharge`, `fact_fee_charge`
  - tokenization
  - credit and fraud features

  The `idp silver`/`idp gold` CLI stages, the Airflow tasks and the Kubernetes Jobs
  all run this code.
- **The execution mode sets only the Spark master and resources, never the
  engine.** Local and CI runs use `local[*]`; the cluster uses its configured master.
  The per-stage `pandas|spark` engine switch in the DAG is removed. **BREAKING**
  for anyone who set `idp_silver_engine`/`idp_gold_engine`.
- **Stages always read their input from the lake**:
  - local: a local warehouse directory
  - cluster: `s3a://<prefix>-<layer>`

  The in-memory frame hand-off between stages (`to_silver(frames)`,
  `to_gold(silver)`, the in-memory training path) is removed. `idp run` and the tests
  compose stages through the lake as well.
- **pandas is confined to ML.** Training reads its (sampled) labelled Gold feature
  set into pandas; scoring uses pandas per batch. No pandas code remains in the
  Silver or Gold transforms.
- **Out of scope for this change:**
  - The synthetic generator and the Bronze landing (`idp consume` writing Kafka
    batches) stay on pandas. They are data sources, not transforms, and Bronze
    ingestion moves to Spark Structured Streaming in change 2.
  - The MCP tools keep reading Silver with pandas until change 4.
  - `publish` keeps writing through pandas until change 3.
- **The duplicated code is removed:** `src/idp/curation/silver.py`,
  `src/idp/features/{base,credit_features,fraud_features}.py` (except the
  feature-name lists the models use), and the duplicated logic in
  `pipelines/spark/*_job.py`. The job scripts become thin entrypoints over
  `idp.transforms`, running in a Spark image that contains the `idp` package.
- **REQ-ORC-04 (engine parity) is retired**, because there is only one engine. The
  parity test's reference outputs become golden-file tests of the single
  implementation.
- **ADR-0010 is superseded by a new ADR:** Spark for data transforms, pandas and
  scikit-learn for ML only.

## Roadmap (four changes, in order)

| # | Change | Outcome | Depends on |
|---|---|---|---|
| 1 | `spark-single-engine` (this change) | One PySpark transforms package; stages read the lake; pandas only in ML | none |
| 2 | `iceberg-incremental-lakehouse` | Nessie with a persistent store; Iceberg tables; Bronze partitioned by `(tenant_id, ingest_date)`; incremental Bronze to Silver (Structured Streaming `AvailableNow` + checkpoint) with Iceberg `MERGE`; daily subscriber aggregates feeding the 7/30/90-day features; S3 lifecycle archiving of old Bronze (Bronze stays immutable and is never deleted to avoid reprocessing) | 1 |
| 3 | `distributed-spark-runtime` | Spark Operator with executors; a baked Spark image (`idp`, `hadoop-aws`, Iceberg; no per-run jar download); explicit driver/executor memory (fixes the Spark Silver OOM); batch scoring in Spark via `mapInPandas` with the registry model | 1 (2 for Iceberg I/O) |
| 4 | `mcp-over-trino` | Trino Iceberg catalog on Nessie; MCP analytics tools as tenant-filtered SQL with predicate pushdown instead of reading whole Silver tables into pandas | 2 |

Interactions with in-flight changes:
- `consent-and-tenant-gateway` should apply its consent filter once, in
  `idp.transforms`, after this change. Otherwise it lands in code this change
  deletes.
- `mlflow-model-registry` and `oidc-tenant-auth` are unaffected.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `orchestration`:
  - REQ-ORC-02 drops the per-stage engine switch.
  - REQ-ORC-03: the local `run` composes stages through a local lake on local
    Spark.
  - REQ-ORC-04 (engine parity) is removed.
  - A new requirement adds a single transformation engine, with pandas confined
    to ML.
- `silver-curation`: REQ-SLV-04. The Silver stage always reads Bronze from the lake;
  there is no in-memory input.
- `gold-features`: add a requirement that the Gold stage always reads Silver from
  the lake.
- `ml-models`: REQ-ML-03. The dev/test in-memory training path is replaced by
  training from a local lake.
- `foundation`: REQ-FND-04. The storage abstraction covers Spark (`s3a://` or a
  local warehouse) as well as pandas reads for ML.

## Impact

- **Code:**
  - new `src/idp/transforms/`
  - rewritten `src/idp/pipeline.py` stage functions (create a SparkSession and call
    the transforms)
  - thin `pipelines/spark/*_job.py`
  - removed pandas curation/feature modules
  - `pipelines/airflow/dags/idp_medallion_dag.py` (no engine switch)
- **Dependencies:** `pyspark` moves from the `[spark]` extra to core. Local dev and
  CI need a JDK 11/17.
- **Images:** the `idp` image gains a JRE and pyspark, or the Spark image gains the
  `idp` package. Change 3 finalizes this; this change needs only that the Spark Job
  can import `idp`.
- **Tests:** unit and integration tests run on a session-scoped local Spark. Suite
  runtime goes up (a Spark session starts once per session). The pandas-only fixtures
  (`silver`, `credit_labeled`) are rebuilt through the lake.
- **Performance at prototype size:** small runs are slower on Spark than on pandas
  (JVM start-up). This is accepted in exchange for one code path that scales.
- **Specs:** REQ-ORC-04 removed; REQ-ORC-02/03, REQ-SLV-04, REQ-ML-03 and REQ-FND-04
  modified; new requirements in `orchestration` and `gold-features`.
