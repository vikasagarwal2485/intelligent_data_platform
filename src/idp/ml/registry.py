"""The model store shared by train, Score and the scoring API (REQ-ML-04).

One interface, two implementations, chosen by configuration (:func:`model_store`):

* :class:`MlflowModelStore` when ``mlflow.tracking_uri`` is set
  (``IDP_MLFLOW_TRACKING_URI``). Each ``train`` is one MLflow run holding its
  parameters and both models' hold-out metrics. Every model is registered as a new
  version of ``idp-credit`` / ``idp-fraud``, but only a version that passes its
  quality gate (:func:`gate`) is moved to the ``champion`` alias, and Score and the
  API load ``champion``.
* :class:`ObjectModelStore` otherwise: today's joblib files in the object store
  (``s3://<prefix>-ml``) or the local ML dir. There is no gate: the latest trained
  model is used.

Models are the ``CreditLimitModel`` / ``FraudModel`` dataclasses, stored as joblib
files. In MLflow they are wrapped in a small pyfunc (:class:`JoblibModel`) so the
registry can version them (``idp.ml.pyfunc_model``, imported only by the MLflow
store), and :meth:`MlflowModelStore.load_champion` unwraps the original object, keeping ``recommend_limit``, ``score`` and ``reason_codes``.
"""
from __future__ import annotations

import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import joblib

from idp.common import storage
from idp.common.config import PlatformConfig

MODELS = ("credit", "fraud")
CHAMPION = "champion"
_FILES = {"credit": "credit_model.joblib", "fraud": "fraud_model.joblib"}


def registered_name(name: str) -> str:
    """The registry name of a model: ``credit`` -> ``idp-credit``."""
    return f"idp-{name}"


# --------------------------------------------------------------------------- #
# Quality gate                                                                 #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GateResult:
    passed: bool
    reason: str


def gate(cfg: PlatformConfig, name: str, metrics: dict[str, Any]) -> GateResult:
    """Whether a newly trained model may become ``champion``.

    Credit passes when its hold-out AUC >= ``credit.min_auc``; fraud when its
    hold-out PR-AUC >= ``fraud.min_pr_auc_lift`` x the hold-out fraud rate (a lift
    over a random ranking). A missing metric (e.g. a single-class hold-out) fails.
    """
    if name == "credit":
        need = float(cfg.credit.get("min_auc", 0.55))
        auc = metrics.get("auc")
        if auc is None:
            return GateResult(False, "no hold-out AUC (single-class hold-out)")
        ok = float(auc) >= need
        return GateResult(ok, f"auc {float(auc):.4f} {'>=' if ok else '<'} min_auc {need}")
    if name == "fraud":
        lift = float(cfg.fraud.get("min_pr_auc_lift", 1.5))
        pr_auc, rate = metrics.get("pr_auc"), metrics.get("fraud_rate")
        if pr_auc is None or rate is None:
            return GateResult(False, "no hold-out PR-AUC (single-class hold-out)")
        need = lift * float(rate)
        ok = float(pr_auc) >= need
        return GateResult(ok, f"pr_auc {float(pr_auc):.4f} {'>=' if ok else '<'} "
                              f"{lift} x fraud_rate {float(rate):.4f} = {need:.4f}")
    raise ValueError(f"unknown model {name!r}; expected one of {MODELS}")


@dataclass(frozen=True)
class SaveResult:
    """What ``save`` did with one model: its version (registry version, or the
    artifact's identity in the object store), and whether it is now the one Score
    and the API load."""
    version: str | None
    promoted: bool
    gate: GateResult


# --------------------------------------------------------------------------- #
# Interface                                                                    #
# --------------------------------------------------------------------------- #
class ModelStore(Protocol):
    def save(self, models: dict[str, tuple[Any, dict[str, Any]]],
             params: dict[str, Any]) -> dict[str, SaveResult]:
        """Persist the models of one training run (``{name: (model, metrics)}``)."""

    def load_champion(self, name: str) -> Any | None:
        """The model Score and the API should use, or None if there is none."""

    def champion_version(self, name: str) -> str | None:
        """A cheap identity of the current champion (to detect a new one), or None."""


def model_store(cfg: PlatformConfig, *, artifacts_dir: str | Path | None = None) -> ModelStore:
    """The configured store: MLflow when a tracking URI is set, else the object store.
    ``artifacts_dir`` forces a local object store (tests, the API's override)."""
    if artifacts_dir is None and cfg.mlflow.get("tracking_uri"):
        return MlflowModelStore(cfg)
    return ObjectModelStore(cfg, artifacts_dir)


def build_tags() -> dict[str, str]:
    """Which code trained a model: unpickling needs the same ``idp`` classes."""
    import idp

    sha = os.environ.get("IDP_GIT_SHA", "")
    if not sha:
        try:
            sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True,
                                 cwd=Path(__file__).parent, timeout=5, check=True).stdout.strip()
        except Exception:  # noqa: BLE001 - no git (e.g. in the image without IDP_GIT_SHA)
            sha = "unknown"
    return {"idp.version": idp.__version__, "idp.git_sha": sha}


# --------------------------------------------------------------------------- #
# Object store (fallback)                                                      #
# --------------------------------------------------------------------------- #
class ObjectModelStore:
    """Joblib files in ``s3://<prefix>-ml`` (S3 enabled) or the local ML dir.
    No registry: every save replaces the model, and the latest one is used."""

    def __init__(self, cfg: PlatformConfig, artifacts_dir: str | Path | None = None):
        self.cfg, self.artifacts_dir = cfg, artifacts_dir

    def uri(self, name: str) -> str:
        if self.artifacts_dir is not None:
            return str(Path(self.artifacts_dir) / _FILES[name])
        return storage.model_uri(self.cfg, _FILES[name])

    def save(self, models, params):
        out = {}
        for name, (model, metrics) in models.items():
            storage.save_model(model, self.uri(name), self.cfg)
            out[name] = SaveResult(self.champion_version(name), True, gate(self.cfg, name, metrics))
        return out

    def load_champion(self, name):
        uri = self.uri(name)
        return storage.load_model(uri, self.cfg) if storage.model_exists(uri, self.cfg) else None

    def champion_version(self, name):
        """The artifact's ETag or modification time: changes when a model is saved."""
        import fsspec

        uri = self.uri(name)
        try:
            fs, path = fsspec.core.url_to_fs(uri, **(storage._model_opts(uri, self.cfg) or {}))
            info = fs.info(path)
        except FileNotFoundError:
            return None
        for key in ("ETag", "LastModified", "mtime", "created"):
            if info.get(key) is not None:
                return f"{info[key]}:{info.get('size')}"
        return str(info.get("size"))


# --------------------------------------------------------------------------- #
# MLflow registry                                                              #
# --------------------------------------------------------------------------- #
class MlflowModelStore:
    """Registered versions with a gated ``champion`` alias (MLflow)."""

    def __init__(self, cfg: PlatformConfig):
        import mlflow
        from mlflow.tracking import MlflowClient

        self.cfg = cfg
        m = cfg.mlflow
        self.uri = m["tracking_uri"]
        mlflow.set_tracking_uri(self.uri)
        mlflow.set_registry_uri(self.uri)
        self.client = MlflowClient(tracking_uri=self.uri, registry_uri=self.uri)
        self.experiment = m.get("experiment", "idp-training")
        self.artifact_location = m.get("artifact_location") or None
        # Artifacts go straight to the object store when the server hands out s3://
        # locations: give the MLflow client the platform's S3 endpoint and keys.
        s3 = cfg.s3
        if s3.get("enabled"):
            os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", str(s3["endpoint"]))
            os.environ.setdefault("AWS_ACCESS_KEY_ID", str(s3["access_key"]))
            os.environ.setdefault("AWS_SECRET_ACCESS_KEY", str(s3["secret_key"]))

    def _experiment_id(self) -> str:
        exp = self.client.get_experiment_by_name(self.experiment)
        if exp is not None:
            return exp.experiment_id
        return self.client.create_experiment(self.experiment, artifact_location=self.artifact_location)

    def save(self, models, params):
        import mlflow
        import pandas as pd
        import sklearn

        from idp.ml.pyfunc_model import JoblibModel

        results: dict[str, SaveResult] = {}
        with mlflow.start_run(experiment_id=self._experiment_id(), run_name="train") as run:
            mlflow.set_tags(build_tags())
            mlflow.log_params({k: v for k, v in params.items() if v is not None})
            for name, (model, metrics) in models.items():
                numeric = {f"{name}_{k}": float(v) for k, v in metrics.items()
                           if isinstance(v, (int, float)) and not isinstance(v, bool)}
                mlflow.log_metrics(numeric)
                g = gate(self.cfg, name, metrics)
                mlflow.set_tags({f"{name}.gate": "passed" if g.passed else "failed",
                                 f"{name}.gate_reason": g.reason})
                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / _FILES[name]
                    joblib.dump(model, path)
                    info = mlflow.pyfunc.log_model(
                        name=name, python_model=JoblibModel(), artifacts={"model": str(path)},
                        registered_model_name=registered_name(name),
                        pip_requirements=[f"scikit-learn=={sklearn.__version__}",
                                          f"pandas=={pd.__version__}", f"joblib=={joblib.__version__}"])
                version = str(info.registered_model_version)
                reg = registered_name(name)
                for k, v in {**{f"metric.{k}": f"{v:.6g}" for k, v in numeric.items()},
                             "gate": "passed" if g.passed else "failed", "gate_reason": g.reason,
                             "run_id": run.info.run_id}.items():
                    self.client.set_model_version_tag(reg, version, k, v)
                if g.passed:
                    self.client.set_registered_model_alias(reg, CHAMPION, version)
                results[name] = SaveResult(version, g.passed, g)
        return results

    def champion_version(self, name):
        from mlflow.exceptions import MlflowException

        try:
            return str(self.client.get_model_version_by_alias(registered_name(name), CHAMPION).version)
        except MlflowException as e:
            if _is_missing(e):
                return None                                   # no model, or no champion yet
            raise

    def load_champion(self, name):
        import mlflow
        from mlflow.exceptions import MlflowException

        try:
            loaded = mlflow.pyfunc.load_model(f"models:/{registered_name(name)}@{CHAMPION}")
        except MlflowException as e:
            if _is_missing(e):
                return None
            raise
        return loaded.unwrap_python_model().model


def _is_missing(e: Exception) -> bool:
    code = getattr(e, "error_code", "")
    return code in ("RESOURCE_DOES_NOT_EXIST", "INVALID_PARAMETER_VALUE", "NOT_FOUND") \
        or "not found" in str(e).lower() or "does not exist" in str(e).lower()
