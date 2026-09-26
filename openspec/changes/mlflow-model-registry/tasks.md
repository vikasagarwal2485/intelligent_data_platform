# Tasks

## 1. Configuration and dependency

- [ ] 1.1 Add an `mlflow` config section (`tracking_uri`, `refresh_seconds`, experiment name) with `IDP_MLFLOW_TRACKING_URI` override, and gate thresholds `credit.min_auc`/`fraud.min_pr_auc_lift` in `platform.yaml`. Verify with a config test for the env override
- [ ] 1.2 Add `mlflow-skinny` and `boto3` to `requirements.txt`. Verify that `pip install -e .[dev]` succeeds and `import mlflow` works

## 2. Model store abstraction

- [ ] 2.1 Create `src/idp/ml/registry.py` with the `ModelStore` interface and an `ObjectModelStore` wrapping the current joblib functions. Verify that the existing tests pass through it (SCN-ML-04.1, 04.6)
- [ ] 2.2 Implement `MlflowModelStore`: log params and metrics, log a pyfunc wrapper around the joblib artifact, register the version, gate, set the `champion` alias, and have `load_champion` unwrap the Python model. Verify with tests against a local file-based MLflow (`tracking_uri=file://tmp`, sqlite registry) (SCN-ML-04.2)
- [ ] 2.3 Test the gate: a passing model becomes champion (SCN-ML-04.3); a failing model is registered but the champion is unchanged (SCN-ML-04.4)

## 3. Wire into stages and API

- [ ] 3.1 `pipeline.train` saves through the store with metrics and returns the gate outcome; `to_scores` loads the champion and skips the table when there is none. Verify with pipeline tests in both modes
- [ ] 3.2 The API caches `(version, model)` and re-checks the champion every `refresh_seconds`, keeping the cached model if the check fails. Verify with a test that promotes a new version and asserts the API uses it after the interval, with no restart (SCN-ML-04.5)
- [ ] 3.3 Log the `idp` version and git SHA as run tags. Verify that the tags are present on the test run

## 4. Deployment

- [ ] 4.1 Add `deploy/platform/mlflow/mlflow-values.yaml` (Postgres backend DB `mlflow` on TimescaleDB, `s3://idp-ml/mlflow` artifact root, MinIO endpoint), and use it in `install-infra.sh`. Verify with `helm upgrade --install` and that the MLflow UI lists the experiment after a train run
- [ ] 4.2 Add the MLflow env to the train-job, score-job and scoring-api manifests and to the DAG `S3_ENV`. Verify with `make train score`: the MLflow UI shows the versions and `champion`, and `make timescale-verify` shows the scores
- [ ] 4.3 Confirm and record the MLflow service host and port from the chart (design Open Question) in `config/platform.yaml` comments and `deploy/README.md`
