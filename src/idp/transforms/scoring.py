"""Distributed scoring (REQ-ML-07): Gold features -> score tables, per partition.

The trained models are small, so they are broadcast to the Spark workers, and each
Arrow batch of the feature table is scored with ``mapInPandas``. pandas only ever
holds one batch, and no process holds the whole feature table (REQ-ORC-05).

``FraudModel`` normalizes its IsolationForest anomaly score by a min/max. Taken
per batch, that would differ from scoring the whole table at once, so a first
pass computes the population min/max of the raw anomaly score (a Spark aggregate),
and every batch is normalized with it. The result equals in-process scoring of the
whole table (SCN-ML-07.5).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator

import pandas as pd

from idp.common import storage
from idp.common.config import PlatformConfig

CREDIT_SCHEMA = ("tenant_id string, service_id string, pd double, "
                 "affordability_base double, recommended_limit double")
FRAUD_SCHEMA = ("tenant_id string, service_id string, fraud_proba double, "
                "anomaly_score double, rule_score double, fraud_score double")
SCORE_PARTITION_COLUMN = "scored_at"


def load_scoring_models(cfg: PlatformConfig, models: dict | None = None) -> dict[str, Any]:
    """The models to score with: ``models`` entries first (tests), else the model
    store. A model absent from both is simply not returned (graceful skip).

    This is the one seam for model loading; a model registry replaces the store
    lookup here."""
    found = {k: v for k, v in (models or {}).items() if v is not None}
    for key, name in (("credit", "credit_model.joblib"), ("fraud", "fraud_model.joblib")):
        uri = storage.model_uri(cfg, name)
        if key not in found and storage.model_exists(uri, cfg):
            found[key] = storage.load_model(uri, cfg)
    return found


def _credit_batches(model_bc):
    def score(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        model = model_bc.value
        for batch in batches:
            if len(batch):
                yield model.recommend_limit(batch).astype({"pd": float, "affordability_base": float,
                                                           "recommended_limit": float})
    return score


def _fraud_raw(model_bc):
    def raw(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        model = model_bc.value
        for batch in batches:
            if len(batch):
                yield pd.DataFrame({"raw": model.raw_anomaly(batch)})
    return raw


def _fraud_batches(model_bc, anomaly_range: tuple[float, float]):
    def score(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        model = model_bc.value
        for batch in batches:
            if len(batch):
                out = model.score(batch, anomaly_range=anomaly_range)
                yield out.astype({c: float for c in ("fraud_proba", "anomaly_score",
                                                     "rule_score", "fraud_score")})
    return score


def score_features(spark: Any, models: dict[str, Any], features: dict[str, Any],
                   scored_at: datetime) -> dict[str, Any]:
    """Score Spark feature DataFrames with the given models. Returns lazy Spark
    DataFrames ``{"credit_scores": …, "fraud_scores": …}`` (only for models present),
    every row stamped with the one driver-side ``scored_at``."""
    from pyspark.sql import functions as F

    stamp = F.lit(scored_at).cast("timestamp")
    out: dict[str, Any] = {}
    credit_df = features.get("credit_features")
    if models.get("credit") is not None and credit_df is not None:
        bc = spark.sparkContext.broadcast(models["credit"])
        out["credit_scores"] = (credit_df.mapInPandas(_credit_batches(bc), CREDIT_SCHEMA)
                                .withColumn(SCORE_PARTITION_COLUMN, stamp))
    fraud_df = features.get("fraud_features")
    if models.get("fraud") is not None and fraud_df is not None and not fraud_df.isEmpty():
        bc = spark.sparkContext.broadcast(models["fraud"])
        rng = fraud_df.mapInPandas(_fraud_raw(bc), "raw double").agg(
            F.min("raw").alias("lo"), F.max("raw").alias("hi")).first()
        out["fraud_scores"] = (fraud_df.mapInPandas(_fraud_batches(bc, (float(rng["lo"]), float(rng["hi"]))),
                                                    FRAUD_SCHEMA)
                               .withColumn(SCORE_PARTITION_COLUMN, stamp))
    return out
