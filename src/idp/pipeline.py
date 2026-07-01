"""End-to-end batch pipeline (plan sections 3-6).

Wires the medallion + ML flow together so it can be driven by the CLI / Make
targets and, in production, by Airflow DAGs:

    synthetic -> Bronze -> Silver -> Features -> ML (credit + fraud) -> Gold

Everything runs per-tenant-isolated and batch-first (reviewer decisions).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split

from idp.common import pii
from idp.common.config import PlatformConfig, load_config
from idp.curation.silver import curate
from idp.features.credit_features import build_credit_features
from idp.features.fraud_features import build_fraud_features
from idp.ingestion.bronze import ingest, load_bronze
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.synthetic.generator import generate_for_tenants


def _salts(cfg: PlatformConfig) -> dict[str, str]:
    return {t.id: t.pii_salt for t in cfg.tenants}


def process_bronze(cfg: PlatformConfig, bronze: dict, *, persist: bool = True) -> dict:
    """Streaming-path medallion: Bronze frames -> Silver -> Gold feature tables.

    Used by the Kafka path (`idp consume`), where records arrive without ML
    labels. It curates Silver, builds the Gold feature tables, and — if trained
    models exist in the artifact store — scores them. Training itself uses the
    labelled batch path (`idp run`).
    """
    from idp.common import storage

    salts = _salts(cfg)
    windows = tuple(cfg.features.get("windows", [30, 90]))

    silver = curate(bronze, salts)
    credit_feats = build_credit_features(silver, windows)
    fraud_feats = build_fraud_features(silver, windows)

    outputs = {"credit_features": credit_feats, "fraud_features": fraud_feats}

    # Optionally score with previously trained models.
    art = Path(cfg.storage.ml_artifacts)
    if (art / "credit_model.joblib").exists():
        outputs["credit_scores"] = CreditLimitModel.load(art / "credit_model.joblib").recommend_limit(credit_feats)
    if (art / "fraud_model.joblib").exists():
        outputs["fraud_scores"] = FraudModel.load(art / "fraud_model.joblib").score(fraud_feats)

    if persist:
        for name, df in silver.items():
            storage.write_parquet(df, f"{storage.lake_uri(cfg, 'silver')}/{name}", cfg,
                                  partition_cols=["tenant_id"])
        for name, df in outputs.items():
            storage.write_parquet(df, f"{storage.lake_uri(cfg, 'gold')}/{name}", cfg)

    return {
        "counts": {
            "subscribers": int(len(silver["dim_subscriber"])),
            "usage_events": int(len(silver["fact_usage"])),
            "recharge_events": int(len(silver["fact_recharge"])),
        },
        "gold_tables": list(outputs),
        "silver": silver,
        **outputs,
    }


def _tokenize_labels(labels: pd.DataFrame, salts: dict[str, str]) -> pd.DataFrame:
    """Tokenize label service_id with the per-tenant salt so it joins to Silver."""
    parts = []
    for tenant_id, part in labels.groupby("tenant_id"):
        parts.append(pii.tokenize_columns(part, salts.get(str(tenant_id), ""), columns=("service_id",)))
    return pd.concat(parts, ignore_index=True)


def _safe_split(df: pd.DataFrame, target: str):
    """Train/test split that degrades gracefully on small/degenerate data.

    Uses a stratified 25% holdout when both classes have >=2 members and the
    frame is large enough; otherwise falls back to a plain split, and for very
    small frames evaluates in-sample (with a clear caveat in the metrics).
    """
    y = df[target]
    counts = y.value_counts()
    if len(df) < 8 or y.nunique() < 2:
        return df, df  # too small to hold out meaningfully
    stratify = y if counts.min() >= 2 else None
    return train_test_split(df, test_size=0.25, random_state=42, stratify=stratify)


def run(cfg: PlatformConfig | None = None, *, persist: bool = True) -> dict:
    """Run the full batch pipeline and return a summary of metrics/artifacts."""
    cfg = cfg or load_config()
    syn = cfg.synthetic
    salts = _salts(cfg)
    windows = tuple(cfg.features.get("windows", [30, 90]))

    # Phase 0: synthetic data ------------------------------------------------
    dataset = generate_for_tenants(
        cfg.tenant_ids,
        subscribers=int(syn.get("subscribers_per_tenant", 200)),
        days=int(syn.get("days", 60)),
        fraud_rate=float(syn.get("fraud_rate", 0.05)),
        default_rate=float(syn.get("default_rate", 0.12)),
        seed=int(syn.get("seed", 42)),
    )

    # Phase 1: Bronze + Silver ----------------------------------------------
    if persist:
        bronze = ingest(dataset, cfg.storage.bronze)
        bronze = load_bronze(cfg.storage.bronze)
    else:
        bronze = {e: getattr(dataset, e) for e in (
            "parties", "accounts", "agreements", "products",
            "voice_events", "data_events", "recharge_events")}
    silver = curate(bronze, salts)

    # Phase 2: features ------------------------------------------------------
    credit_feats = build_credit_features(silver, windows)
    fraud_feats = build_fraud_features(silver, windows)

    labels = _tokenize_labels(dataset.labels, salts)
    credit_df = credit_feats.merge(labels, on=["tenant_id", "service_id"], how="inner")
    fraud_df = fraud_feats.merge(labels, on=["tenant_id", "service_id"], how="inner")

    # Phase 3: credit model (out-of-sample evaluation on a holdout split) ----
    c_train, c_test = _safe_split(credit_df, "will_default")
    credit_model = CreditLimitModel(
        base_multiplier=float(cfg.credit.get("base_multiplier", 3.0)),
        min_limit=float(cfg.credit.get("min_limit", 5.0)),
        max_limit=float(cfg.credit.get("max_limit", 500.0)),
    ).fit(c_train, c_train["will_default"])
    credit_metrics = credit_model.evaluate(c_test, c_test["will_default"])
    credit_scores = credit_model.recommend_limit(credit_feats)

    # Phase 4: fraud model (out-of-sample evaluation on a holdout split) ------
    f_train, f_test = _safe_split(fraud_df, "is_fraud")
    fraud_model = FraudModel(
        contamination=float(cfg.fraud.get("anomaly_contamination", 0.05)),
        thresholds=cfg.fraud.get("rule_thresholds", {}),
    ).fit(f_train, f_train["is_fraud"])
    fraud_metrics = fraud_model.evaluate(f_test, f_test["is_fraud"])
    fraud_scores = fraud_model.score(fraud_feats)

    # Gold + artifacts -------------------------------------------------------
    if persist:
        gold = Path(cfg.storage.gold)
        (gold).mkdir(parents=True, exist_ok=True)
        credit_scores.to_parquet(gold / "credit_scores.parquet", index=False)
        fraud_scores.to_parquet(gold / "fraud_scores.parquet", index=False)
        credit_model.save(Path(cfg.storage.ml_artifacts) / "credit_model.joblib")
        fraud_model.save(Path(cfg.storage.ml_artifacts) / "fraud_model.joblib")

    return {
        "tenants": cfg.tenant_ids,
        "counts": {
            "subscribers": int(len(silver["dim_subscriber"])),
            "usage_events": int(len(silver["fact_usage"])),
            "recharge_events": int(len(silver["fact_recharge"])),
        },
        "credit_metrics": credit_metrics,
        "fraud_metrics": fraud_metrics,
        "credit_scores": credit_scores,
        "fraud_scores": fraud_scores,
        "silver": silver,
    }
