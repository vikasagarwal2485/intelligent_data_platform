# Tasks

## 1. Capture golden outputs (before any pandas code is removed)

- [x] 1.1 Add `tests/golden/make_golden.py`: generate the fixed dataset (tenants a/b, 20 subscribers, 10 days, seed 3), run the **current** pandas `to_silver`/`to_gold` into a temp lake, and copy the four Silver tables and two Gold feature tables to `tests/golden/{silver,gold}/`. Verify that the files exist and total under 1 MB
- [x] 1.2 Add `tests/test_golden.py` with the comparator from `test_spark_parity.py` (same columns and order, logical types, integers exact, reals within 1e-6). It builds the same Bronze and runs the Silver and Gold stages. Verify that it passes against the current pandas stages (baseline)

## 2. Configuration and Spark runtime

- [x] 2.1 Move `pyspark==3.5.3` from the `[spark]` extra into core dependencies (`pyproject.toml`, `requirements.txt`). Verify with `pip install -e ".[dev]"` and `python -c "import pyspark"`
- [x] 2.2 Add a `spark` config section (`master: local[*]`, `driver_memory: 1g`) to `platform.yaml`, with `IDP_SPARK_MASTER`/`IDP_SPARK_DRIVER_MEMORY` overrides in `PlatformConfig.spark`. Verify with config tests (SCN-FND-01.1, SCN-FND-01.4)
- [x] 2.3 Create `src/idp/transforms/session.py` (`get_spark(cfg)`: master, driver memory, UTC, dynamic overwrite, TIMESTAMP_MICROS, s3a settings when S3 is enabled, `getOrCreate`) and `lake.py` (`layer_uri`, `read_table`, `write_table` with per-tenant overwrite). Verify with unit tests for URI resolution (SCN-FND-04.3) and the master override (SCN-ORC-05.2)
- [x] 2.4 Create `src/idp/transforms/tokens.py` (Spark tokenization and `strip_uuid`). Verify with a test that Spark tokens equal `pii.tokenize_value` for the same tenant salt and values, including null handling (SCN-FND-03.3)

## 3. Single transforms implementation

- [x] 3.1 Move `CREDIT_FEATURES`/`FRAUD_FEATURES` into `src/idp/features/__init__.py` and update imports in `ml/*`, `serving/api.py` and the tests. Verify that `pytest tests/test_api.py tests/test_credit_model.py tests/test_fraud_model.py` passes
- [x] 3.2 Port the Silver logic from `pipelines/spark/silver_job.py` into pure functions in `src/idp/transforms/silver.py` (`build_dim_subscriber`, `build_fact_usage`, `build_fact_recharge`, `build_fact_fee_charge`, `curate`). Verify with a unit test per table on a tiny hand-built DataFrame
- [x] 3.3 Port the Gold logic from `pipelines/spark/gold_job.py` into `src/idp/transforms/features.py` (aggregates, `build_credit_features`, `build_fraud_features`, using the shared feature lists). Verify with unit tests for windowed aggregates, fee attribution and the per-tenant z-score
- [x] 3.4 Rewrite `idp.pipeline.to_silver(cfg)`/`to_gold(cfg)`: lake in, lake out, no in-memory parameters, and a summary of names and counts returned. Keep the clear errors for missing Bronze and Silver. Verify that `tests/test_golden.py` passes on the Spark stages and that tests for SCN-SLV-04.1, SCN-SLV-04.2, SCN-GLD-05.1 and SCN-GLD-05.2 pass
- [x] 3.5 Add a test that patches `pyspark.sql.DataFrame.toPandas` to raise while `to_silver`/`to_gold` run. Verify that both complete (SCN-ORC-05.3)
- [x] 3.6 Import pyspark lazily. Verify with a test that importing `idp.serving.api` and `idp.mcp.tools` and running `idp score` on a local lake leaves `pyspark` out of `sys.modules`, or at least starts no SparkContext

## 4. ML and the local run through the lake

- [x] 4.1 Remove the in-memory feature path from `train` (the `dataset`/`silver` parameters). Training always reads Gold features and labels from the lake. Verify with tests for SCN-ML-03.3 and SCN-ML-03.4
- [x] 4.2 Rewrite `run(cfg)` as generate → `to_bronze` → `to_silver` → `to_labels` → `to_gold` → `train` → `to_scores`, all through the lake. `--no-persist` uses a temporary lake that is removed afterwards. Verify with `idp run` and `make smoke`, and with a test for SCN-ORC-03.2 (run's Gold equals separate `silver` + `gold` runs)
- [x] 4.3 Make `to_scores` read Gold features from the lake only; drop its `gold=` in-memory parameter. Keep `models=` for tests. Verify that the score tests pass

## 5. Tests migrated to the single engine

- [x] 5.1 In `tests/conftest.py`, add a session-scoped `spark` fixture and a session-scoped `lake` fixture (Bronze → Silver → Gold once). Rebuild `silver`, `credit_labeled` and `fraud_labeled` as pandas reads from that lake. Verify that the fixture users (`test_features`, `test_credit_model`, `test_fraud_model`, `test_curation`, `test_ml_store_explain`) pass
- [x] 5.2 Update the tests that called `curate`/`build_*_features`, the in-memory `to_silver(frames)`/`to_gold(silver=...)` or `train(dataset=...)` (`test_streaming`, `test_mcp_tools`, `test_idempotent_writes`, `test_pipeline`, `test_api`, `test_ml_store_explain`) to go through the lake. Verify that each file passes
- [x] 5.3 Delete `tests/test_spark_parity.py` (superseded by `test_golden.py`). Verify with a full `pytest -q` run and record the new suite runtime in the PR notes

## 6. Remove the duplicate implementation

- [x] 6.1 Delete `src/idp/curation/silver.py`, and `src/idp/features/{base,credit_features,fraud_features}.py` (the lists live in `__init__`). Keep `curation/jsonapi.py`, which parses JSON:API row by row and isn't a table transform. Verify with `grep -rn "curation.silver\|build_credit_features\|build_fraud_features" src tests pipelines` (only `idp.transforms` hits) and a green `pytest -q`
- [x] 6.2 Reduce `pipelines/spark/{silver,gold}_job.py` to entrypoints that load config and call `idp.pipeline.to_silver/to_gold`. Verify with `python pipelines/spark/silver_job.py` then `gold_job.py` against a local lake

## 7. Image, jobs, Airflow and CI

- [x] 7.1 `Dockerfile`: install `openjdk-17-jre-headless`, set `JAVA_HOME`, and download `hadoop-aws-3.3.4` and `aws-java-sdk-bundle-1.12.262` into pyspark's `jars/` at build time. Verify that `make image` succeeds and that `docker run idp/scoring-api:dev python -c "from idp.transforms.session import get_spark; ..."` starts Spark with no downloads
- [x] 7.2 `deploy/app/jobs/{silver,gold}-job.yaml`: resources request 1.5Gi / limit 3Gi and `IDP_SPARK_DRIVER_MEMORY=1g`. Delete `spark-{silver,gold}-job.yaml`. Verify with `kubectl apply --dry-run=client -k deploy/app/overlays/local`
- [x] 7.3 `pipelines/airflow/dags/idp_medallion_dag.py`: remove `_engine`, `_spark_stage`, `SPARK_SALTS`, `_silver_task` and `_gold_task`, and make silver and gold use `_stage` with the Spark resources and env. Update the module docstring. Verify that the DAG imports cleanly (`python -c` with Airflow stubs, or `make airflow-reload` and no import errors in the UI) and that the tasks are `silver >> gold` (SCN-ORC-02.3)
- [x] 7.4 `Makefile`: remove `spark-configmap`, `spark-silver`, `spark-gold`, `silver-engine` and `gold-engine`, and add a `java -version` check to `install`. Verify with `make help`
- [ ] 7.5 `.github/workflows/ci.yml`: add `actions/setup-java@v4` (Temurin 17) before the install step. Verify that the workflow YAML is valid (`python -c "import yaml; yaml.safe_load(open(...))"`) and that CI is green on push
- [x] 7.6 Set `max_active_runs=1` on the `idp_features` DAG (from cluster verification: overlapping runs raced on Silver and stacked Spark pods). Verify with the stub DAG check (`max_active_runs == 1`) and in 9.2 that no two features-DAG Silver/Gold pods run at once (SCN-ORC-02.4)

## 8. Docs and decisions

- [x] 8.1 Add an ADR to `docs/decisions/decision-log.md`: "Spark for data transforms; pandas and scikit-learn for ML only", superseding ADR-0010, with ADR-0010 marked Superseded. Verify by reading the decision log
- [x] 8.2 Update `README.md` and `deploy/README.md`: the JDK prerequisite, the removed engine switch and Makefile targets, and the migration steps from design.md. Verify that the docs mention no removed target (`grep -n "spark-configmap\|silver-engine\|gold-engine" README.md deploy/README.md` is empty)

## 9. Cluster verification

- [x] 9.1 On minikube: `make image`, `make app-deploy`, `make airflow-reload`, then delete the `idp_silver_engine`/`idp_gold_engine` Variables and the `idp-silver-spark`/`idp-gold-spark` ConfigMaps. Run `make silver gold score publish`, then verify with `make timescale-verify` and an in-cluster duplicate check that the serving tables hold one row per key and that no `apache/spark` pod was created
- [x] 9.2 Unpause `idp_features`/`idp_inference`, trigger `idp_features`, and verify that `idp_inference` succeeds and that the Silver and Gold pods stay under their memory limit (`kubectl top`, no OOMKilled). Then restore the DAGs' paused state
