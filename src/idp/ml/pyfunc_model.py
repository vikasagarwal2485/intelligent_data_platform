"""The MLflow pyfunc wrapper of the registry's models (REQ-ML-04).

Separate from ``registry.py`` because importing ``mlflow.pyfunc`` pulls in parts of
pyspark: only a registry save or load imports this module, so the API, MCP and CLI
entrypoints stay free of it.
"""
from __future__ import annotations

from typing import Any

import joblib
import mlflow.pyfunc
import pandas as pd


class JoblibModel(mlflow.pyfunc.PythonModel):
    """A pyfunc around a joblib-pickled ``idp`` model (``CreditLimitModel`` or
    ``FraudModel``). The registry only needs a loadable, versioned artifact; callers
    unwrap the original object (``unwrap_python_model().model``)."""

    def load_context(self, context) -> None:
        self.model = joblib.load(context.artifacts["model"])

    def predict(self, context, model_input: pd.DataFrame, params: dict[str, Any] | None = None) -> pd.DataFrame:
        model = self.model
        return model.score(model_input) if hasattr(model, "score") else model.recommend_limit(model_input)
