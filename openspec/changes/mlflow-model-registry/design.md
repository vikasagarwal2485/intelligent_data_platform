# Design

## Context

- `storage.save_model`/`load_model`/`model_exists` pickle `CreditLimitModel` and
  `FraudModel` (dataclasses wrapping sklearn estimators) with joblib to
  `model_uri(cfg, "<name>.joblib")`.
- Callers: `pipeline.train` (save), `pipeline.to_scores` (load if present), and
  `serving/api.py` (lazy load, then cached in `state` forever).
- `install-infra.sh` runs `helm upgrade --install mlflow community-charts/mlflow`
  with default values: an ephemeral local backend and artifact store.
- The Airflow `MODEL` dataset URI is a logical id and is unaffected.

## Goals / Non-Goals

**Goals:**
- Registry-backed versions with metrics, a gated `champion` alias, and champion
  loading in Score and the API.
- A zero-config fallback, so local dev and tests need no MLflow server.

**Non-Goals:**
- Manual approval workflows or UI-driven promotion. The gate is automatic; a human
  can still move the alias in the MLflow UI.
- Model signature/schema enforcement and serving through the MLflow pyfunc server.
- Experiment comparison dashboards beyond the MLflow UI.

## Decisions

- **A `ModelStore` interface in a new `src/idp/ml/registry.py`**, with methods
  `save(name, model, metrics, params) -> version | None`, `load_champion(name)`,
  `champion_version(name)`, and two implementations:
  - `MlflowModelStore`, selected when `cfg.mlflow["tracking_uri"]` is set (env
    `IDP_MLFLOW_TRACKING_URI`)
  - `ObjectModelStore`, which wraps today's `storage.*_model` functions

  `pipeline` and `api` depend only on the interface.
  - Alternative: inline `if mlflow:` branches. Rejected because of three call sites
    and harder tests.
- **Packaging: an `mlflow.pyfunc` wrapper around the joblib artifact.** Log a
  `PythonModel` whose `load_context` joblib-loads the dataclass. Callers use
  `unwrap_python_model()` to get the real `CreditLimitModel`/`FraudModel`, keeping
  `recommend_limit`, `score` and `reason_codes` intact.
  - Alternative: the `mlflow.sklearn` flavor. Rejected because the models are
    composite dataclasses, not single estimators.
- **Gate** in `train` after evaluation:
  - credit passes when `auc >= credit.min_auc` (default 0.6)
  - fraud passes when `pr_auc >= fraud.min_pr_auc_lift * fraud_rate` (default 1.5)
  - a missing metric (single-class hold-out) fails the gate

  On pass, `set_registered_model_alias(name, "champion", version)`. Metrics and the
  gate outcome are logged as run tags.
- **Champion refresh in the API**: cache `(version, model)`. On each request, if
  more than `mlflow.refresh_seconds` (default 60) have passed since the last check,
  call `get_model_version_by_alias`; on a version change, load the new model. A
  failed check keeps the cached model (serve stale rather than 503). The object-store
  fallback does the same using the artifact's mtime/ETag.
- **Deploy.** `deploy/platform/mlflow/mlflow-values.yaml`:
  - backend store is the TimescaleDB Postgres, database `mlflow` (created like the
    Airflow DB)
  - `--default-artifact-root s3://idp-ml/mlflow`, with `MLFLOW_S3_ENDPOINT_URL`
    pointing at MinIO

  Clients get `IDP_MLFLOW_TRACKING_URI`, `MLFLOW_S3_ENDPOINT_URL` and AWS creds.
- **Dependency**: `mlflow-skinny` + `boto3` in `requirements.txt`. The skinny client
  supports tracking, registry and pyfunc without the server extras.

## Risks / Trade-offs

- [Unpickling needs the same `idp` code version as training] → Log the `idp`
  `__version__` and git SHA as run tags. A class-shape mismatch fails loudly at load
  time, and the API keeps the old cached model.
- [MLflow outage at Score time] → Score fails the task (Airflow retries). No silent
  fallback to the object store in registry mode, because mixing sources hides
  drift.
- [Gate thresholds on tiny synthetic hold-outs are noisy] → Configurable, with
  defaults well below the observed metrics. Tests use explicit thresholds.
- [Image size grows with mlflow-skinny and boto3] → Acceptable (tens of MB).

## Migration Plan

1. Install MLflow with the new values (backend DB, S3 artifact root), then
   `make image`.
2. Set `IDP_MLFLOW_TRACKING_URI` on the train/score/api workloads and the DAG pods.
3. Run `make train`. Verify in the MLflow UI that the versions exist and
   `champion` is set, then `make score`.
4. Rollback: unset `IDP_MLFLOW_TRACKING_URI`. Workloads fall back to the object
   store; the old joblib files are still written by the fallback path only.

## Open Questions

- The exact in-cluster MLflow service host and port from the community chart
  (e.g. `http://mlflow.idp-platform.svc:80` vs `:5000`). This is a config value
  confirmed at deploy time.
