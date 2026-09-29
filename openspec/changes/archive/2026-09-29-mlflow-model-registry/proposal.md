# Proposal

## Why

The plan and the old REQ-ML-03 said models go "to MLflow in production", and
`install-infra.sh` installs an MLflow server, but nothing uses it. Models are joblib
files overwritten in place at `s3://idp-ml/*.joblib`. That gives:

- no run history or metrics lineage
- no versioning or rollback
- no promotion gate: any trained model, however poor, is immediately used by Score
  and the API

The scoring API also caches the first model it loads for its whole lifetime, so a
retrained model never reaches on-demand scoring without a restart.

## What Changes

- `train` logs each run to MLflow (parameters, hold-out metrics, model artifacts)
  and registers new versions of `idp-credit` and `idp-fraud`.
- A version is promoted to the `champion` alias only if it passes a configurable
  quality gate. A failing model is registered but not promoted.
- The Score stage and the scoring API load the `champion` version. The API picks up
  a newly promoted champion without a restart, within a bounded refresh interval.
- When MLflow is not configured (local dev, tests), behaviour falls back to today's
  object-store/local joblib store.
- The MLflow server is configured with a persistent backend and the `idp-ml` bucket
  as its artifact store.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ml-models`: REQ-ML-04 becomes "model registry with a promoted champion" (MLflow),
  with the object store as fallback.

## Impact

- Code: `src/idp/common/storage.py` or a new `src/idp/ml/registry.py`;
  `src/idp/pipeline.py` (train/score); `src/idp/serving/api.py` (champion loading
  and refresh); `src/idp/common/config.py` (`mlflow` section, `IDP_MLFLOW_*` env).
- Dependencies: `mlflow-skinny` (plus `boto3` for S3 artifacts).
- Deploy: MLflow Helm values (backend store on TimescaleDB/Postgres, S3 artifact
  root on MinIO); `IDP_MLFLOW_TRACKING_URI` on the train/score Jobs, the Airflow
  stage pods and the scoring-api Deployment.
- Orchestration: unchanged. The training DAG still produces the model dataset, and
  inference re-scores using whatever is `champion`.
