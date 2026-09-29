# Tasks

## 1. Profiles and memory

- [x] 1.1 Add `spark.profiles.<stage>` (silver, gold, score, publish, maintain, train) to `config/platform.yaml` with the design's minikube defaults, and have `get_spark` apply driver settings (and executor settings when the master isn't local). Verify with config and `spark_conf` unit tests (SCN-ORC-05.2, SCN-ORC-07.1 at conf level)
- [x] 1.2 Add `make mk-check`: compare the minikube node container's memory cap (`docker inspect minikube`) with the node's reported memory and with the largest stage profile's total pod memory plus the platform footprint; fail with the `docker update --memory … minikube` fix. Document it in `deploy/README.md` next to the existing OOM note. Verify that it passes on the 12 GB node and fails when a profile is raised beyond the cap

## 2. Distributed scoring

- [x] 2.1 Add `FraudModel.score(X, anomaly_range=None)` (default unchanged) and a helper for the raw anomaly score. Verify with a test that in-process `score(X)` output is unchanged
- [x] 2.2 Create `transforms/scoring.py`: `load_scoring_models(cfg)`, the global anomaly-range pass, and `mapInPandas` scoring with broadcast models and a driver-side `scored_at`, appending to the Iceberg score tables. Rewire `to_scores` to it. Verify with tests for SCN-ML-07.5 (distributed == in-process on the same features), 07.2, 07.3 and 07.4, and that `test_consent.py` / `test_idempotent_writes.py` score tests pass
- [x] 2.3 Test that Score never calls `toPandas` on a whole table (patch `DataFrame.toPandas` to raise) (SCN-ORC-05.3)

## 3. Distributed publish

- [x] 3.1 Factor `timescale.swap_in(conn, table)` (advisory lock, unique index on staging, drop, rename) and test it with SQLite plus the transactional-DDL recipe: duplicate keys keep the old table; the swap is atomic (SCN-SRV-05.2, SCN-SRV-01.5 at unit level)
- [x] 3.2 Rewrite `publish_serving` to compute the latest run per tenant in Spark, write to `gold.<t>__staging` over JDBC, then `swap_in`, using the PostgreSQL JDBC driver already in the image. Verify with a local test against an ephemeral Postgres if available (`pytest -m postgres`, skipped otherwise), and on minikube in 6.2

## 4. Bounded training

- [x] 4.1 `train`: a Spark join of Gold features and labels, `sampleBy` on a tenant-plus-label stratum with a fixed seed, capped at `ml.train_max_rows`, then `toPandas`. Add `sampled_rows`/`sample_seed` to the metrics. Verify with tests for SCN-ML-09.1 (cap, per-tenant rates within 1 pp) and 09.2 (small set unchanged)

## 5. Spark Operator and deployment

- [x] 5.1 Install the Spark Operator (pinned chart v2.x) via `install-infra.sh` and `make spark-operator`, watching `idp-app`. Add the Spark ServiceAccount and driver RBAC in `idp-app`, and the Airflow `sparkapplications` RBAC. Verify SCN-PLT-10.1 with a hand-applied Gold `SparkApplication`
- [x] 5.2 Add `idp spark-manifest <stage>` (renders a `SparkApplication` from the profile, catalog, S3 and TimescaleDB env), the committed `deploy/app/spark/*.yaml`, and switch `make silver|gold|score|publish|maintain` to apply-and-wait on the application state. Dockerfile: copy `pipelines/spark` into the image. Verify with `kubectl apply --dry-run=server` for every rendered stage
- [x] 5.3 DAG: `SparkKubernetesOperator` for silver, gold, score, publish and maintain (rendered manifests baked into the DAG image), keeping datasets and `max_active_runs=1`. Verify with the stub DAG check and SCN-ORC-02.6 (a failed application fails the task) on minikube
- [x] 5.4 Update `README.md` and `deploy/README.md` (operator, profiles, how to scale a stage), add an ADR (Spark Operator runtime; distributed score and publish; bounded training) to the decision log, and have ADR-0027 reference it. Verify by reading

## 6. Verification

- [x] 6.1 `pytest -q` is green (the golden, consent and idempotency tests unchanged in intent) — 241 passed, 1 skipped (the `postgres`-marked test; no local Postgres), 2026-09-29
- [x] 6.2 On minikube: `make silver gold score publish` as Spark applications. Verify that executor pods ran (SCN-ORC-01.4), the results match the previous run's counts and consent-filtered serving, and `timescale-verify` is OK. Then trigger `idp_features` → `idp_inference` success
- [x] 6.3 Scale check: generate 10× subscribers, raise the Silver/Gold executor counts only, and verify completion with no OOMKilled pods (SCN-ORC-07.2). Run Silver and maintain concurrently and verify no OOM (SCN-ORC-07.3). Record the peak memory per pod — Done 2026-09-29 on minikube with a **deviation (user's choice)**: at 10× the platform uses about 6.5 GB (Kafka grew), so Silver/Gold at 3 executors (about 6 GB) exceed the 12 GB node; `make mk-check` blocked that run as designed, and the check ran at the default 2 executors instead. 10× Bronze (716,971 records landed): Silver 108 s, Gold 77 s, driver + 2 executors each, no OOMKilled container, node oom_kill unchanged (47). Silver fact_usage 694,859, Gold 2,960 (consent-filtered, of 3,295 subscribers). SCN-ORC-07.3: `silver-full-refresh` and `maintain` ran concurrently, both COMPLETED in about 290 s, no OOM; executors of the second stage waited on CPU requests. The full refresh equals the incremental result (694,859 rows, 0 duplicate keys, with 46,981 duplicate keys in Bronze). Peak MiB per pod (limit 1,536): 10× silver driver 1,158 / executors 743, 1,049; gold driver 945 / executors 910, 877; concurrent silver driver 910 / executors 1,109, 1,144; maintain driver 891 / executor 840.
