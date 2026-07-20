"""Medallion pipeline stages (plan sections 3-6).

Each stage is an INDEPENDENT step that reads its input layer and writes its
output layer, so the concerns stay separated (no consumer->silver->gold chaining):

    to_bronze : raw frames                -> Bronze  (s3://idp-bronze / local)
    to_silver : Bronze  -> canonical      -> Silver  (s3://idp-silver / local)
    to_gold   : Silver  -> features/score -> Gold    (s3://idp-gold   / local)
    train     : labelled features -> models (offline ML; labels not in the lake)

Each of to_silver/to_gold reads its input from storage by default, or accepts an
in-memory frame set (used by the local `run` orchestrator and by tests). The
`idp` CLI exposes them as `consume` (Bronze), `silver`, `gold`, `train`; `run`
composes them locally for dev/testing.
"""
from __future__ import annotations

import pandas as pd
from sklearn.model_selection import train_test_split

from idp.common import pii, storage
from idp.common.config import PlatformConfig, load_config
from idp.curation.silver import curate
from idp.features.credit_features import build_credit_features
from idp.features.fraud_features import build_fraud_features
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.synthetic.generator import SyntheticDataset, generate_for_tenants

ENTITIES = (
    "parties", "accounts", "agreements", "products",
    "voice_events", "data_events", "recharge_events", "fee_charges",
)
SILVER_TABLES = ("dim_subscriber", "fact_usage", "fact_recharge", "fact_fee_charge")


def _salts(cfg: PlatformConfig) -> dict[str, str]:
    return {t.id: t.pii_salt for t in cfg.tenants}


def _read_layer(cfg: PlatformConfig, layer: str, tables) -> dict[str, pd.DataFrame]:
    """Read the parquet tables of a lake layer into frames (skips absent tables)."""
    out: dict[str, pd.DataFrame] = {}
    base = storage.lake_uri(cfg, layer)
    for t in tables:
        try:
            df = storage.read_parquet(f"{base}/{t}", cfg)
        except Exception:  # noqa: BLE001 - missing/empty path -> skip
            continue
        if "tenant_id" in df.columns:
            df["tenant_id"] = df["tenant_id"].astype(str)
        out[t] = df
    return out


# --- Stage 1: Bronze --------------------------------------------------------
def to_bronze(cfg: PlatformConfig, frames: dict[str, pd.DataFrame], *, persist: bool = True) -> dict:
    """Persist raw per-entity frames to the Bronze layer (tenant-partitioned)."""
    if persist:
        for name, df in frames.items():
            if df is not None and not df.empty:
                storage.write_parquet(df, f"{storage.lake_uri(cfg, 'bronze')}/{name}", cfg,
                                      partition_cols=["tenant_id"])
    return {"bronze_tables": [n for n, d in frames.items() if d is not None and not d.empty]}


# --- Stage 2: Silver --------------------------------------------------------
def to_silver(cfg: PlatformConfig, bronze: dict | None = None, *, persist: bool = True) -> dict[str, pd.DataFrame]:
    """Bronze -> canonical Silver tables. Reads Bronze from storage if not given."""
    if bronze is None:
        bronze = _read_layer(cfg, "bronze", ENTITIES)
    required = ["parties", "accounts", "agreements"]
    missing = [t for t in required if t not in bronze or bronze[t].empty]
    if missing:
        raise RuntimeError(
            f"Bronze is missing entity tables {missing}. Populate Bronze first "
            "('idp produce' then 'idp consume')."
        )
    silver = curate(bronze, _salts(cfg))
    if persist:
        for name, df in silver.items():
            storage.write_parquet(df, f"{storage.lake_uri(cfg, 'silver')}/{name}", cfg,
                                  partition_cols=["tenant_id"])
    return silver


# --- Stage 3: Gold ----------------------------------------------------------
def to_gold(
    cfg: PlatformConfig,
    silver: dict | None = None,
    *,
    persist: bool = True,
    models: dict | None = None,
) -> dict:
    """Silver -> Gold feature tables (+ ML scores if models are available).

    Reads Silver from storage if not given. Scores are added when ``models`` are
    passed or trained models exist in the artifact store; otherwise Gold contains
    the feature tables only (no labels are present in the lake to train from).
    """
    if silver is None:
        silver = _read_layer(cfg, "silver", SILVER_TABLES)
    if "dim_subscriber" not in silver or silver["dim_subscriber"].empty:
        raise RuntimeError("Silver is empty. Run the Silver stage ('idp silver') first.")

    windows = tuple(cfg.features.get("windows", [30, 90]))
    outputs: dict[str, pd.DataFrame] = {
        "credit_features": build_credit_features(silver, windows),
        "fraud_features": build_fraud_features(silver, windows),
    }

    credit_model = (models or {}).get("credit")
    fraud_model = (models or {}).get("fraud")
    credit_uri = storage.model_uri(cfg, "credit_model.joblib")
    fraud_uri = storage.model_uri(cfg, "fraud_model.joblib")
    if credit_model is None and storage.model_exists(credit_uri, cfg):
        credit_model = storage.load_model(credit_uri, cfg)
    if fraud_model is None and storage.model_exists(fraud_uri, cfg):
        fraud_model = storage.load_model(fraud_uri, cfg)
    if credit_model is not None:
        outputs["credit_scores"] = credit_model.recommend_limit(outputs["credit_features"])
    if fraud_model is not None:
        outputs["fraud_scores"] = fraud_model.score(outputs["fraud_features"])

    if persist:
        for name, df in outputs.items():
            storage.write_parquet(df, f"{storage.lake_uri(cfg, 'gold')}/{name}", cfg,
                                  partition_cols=["tenant_id"])

    return {
        "gold_tables": list(outputs),
        "counts": {"subscribers": int(len(silver["dim_subscriber"]))},
        **outputs,
    }


# --- Stage 4: Serving (Gold -> TimescaleDB) ---------------------------------
GOLD_SERVING_TABLES = ("credit_scores", "fraud_scores", "credit_features", "fraud_features")


def publish_serving(cfg: PlatformConfig, gold: dict | None = None) -> dict:
    """Publish Gold tables to the TimescaleDB serving layer (plan Q4/Q6).

    Reads the Gold tables from storage if not given, then upserts them into
    TimescaleDB indexed by (tenant_id, service_id) for low-latency serving.
    """
    from idp.serving import timescale

    if gold is None:
        gold = _read_layer(cfg, "gold", GOLD_SERVING_TABLES)
    if not gold:
        raise RuntimeError("No Gold tables found to publish. Run the Gold stage ('idp gold') first.")
    published = timescale.publish_gold(cfg, gold)
    return {"published": published, "target": cfg.timescale.get("host")}


# --- Offline ML training (separate concern; needs labels) -------------------
def _tokenize_labels(labels: pd.DataFrame, salts: dict[str, str]) -> pd.DataFrame:
    parts = []
    for tenant_id, part in labels.groupby("tenant_id"):
        parts.append(pii.tokenize_columns(part, salts.get(str(tenant_id), ""), columns=("service_id",)))
    return pd.concat(parts, ignore_index=True)


def _safe_split(df: pd.DataFrame, target: str):
    y = df[target]
    counts = y.value_counts()
    if len(df) < 8 or y.nunique() < 2:
        return df, df
    stratify = y if counts.min() >= 2 else None
    return train_test_split(df, test_size=0.25, random_state=42, stratify=stratify)


def train(
    cfg: PlatformConfig,
    *,
    dataset: SyntheticDataset | None = None,
    silver: dict | None = None,
    persist: bool = True,
) -> dict:
    """Offline model training on a LABELLED dataset (labels are not in the lake).

    Builds features from Silver, joins the ground-truth labels, trains the credit
    and fraud models, evaluates out-of-sample, and saves the models. In production
    this reads a labelled outcomes table and logs to MLflow.
    """
    cfg = cfg or load_config()
    salts = _salts(cfg)
    windows = tuple(cfg.features.get("windows", [30, 90]))
    syn = cfg.synthetic

    if dataset is None:
        dataset = generate_for_tenants(
            cfg.tenant_ids,
            subscribers=int(syn.get("subscribers_per_tenant", 200)),
            days=int(syn.get("days", 60)),
            fraud_rate=float(syn.get("fraud_rate", 0.05)),
            default_rate=float(syn.get("default_rate", 0.12)),
            seed=int(syn.get("seed", 42)),
        )
    if silver is None:
        bronze = {e: getattr(dataset, e) for e in ENTITIES}
        silver = curate(bronze, salts)

    labels = _tokenize_labels(dataset.labels, salts)
    credit_df = build_credit_features(silver, windows).merge(labels, on=["tenant_id", "service_id"])
    fraud_df = build_fraud_features(silver, windows).merge(labels, on=["tenant_id", "service_id"])

    c_train, c_test = _safe_split(credit_df, "will_default")
    credit_model = CreditLimitModel(
        base_multiplier=float(cfg.credit.get("base_multiplier", 3.0)),
        min_limit=float(cfg.credit.get("min_limit", 5.0)),
        max_limit=float(cfg.credit.get("max_limit", 500.0)),
    ).fit(c_train, c_train["will_default"])
    credit_metrics = credit_model.evaluate(c_test, c_test["will_default"])

    f_train, f_test = _safe_split(fraud_df, "is_fraud")
    fraud_model = FraudModel(
        contamination=float(cfg.fraud.get("anomaly_contamination", 0.05)),
        thresholds=cfg.fraud.get("rule_thresholds", {}),
    ).fit(f_train, f_train["is_fraud"])
    fraud_metrics = fraud_model.evaluate(f_test, f_test["is_fraud"])

    if persist:
        storage.save_model(credit_model, storage.model_uri(cfg, "credit_model.joblib"), cfg)
        storage.save_model(fraud_model, storage.model_uri(cfg, "fraud_model.joblib"), cfg)

    return {
        "credit_metrics": credit_metrics,
        "fraud_metrics": fraud_metrics,
        "models": {"credit": credit_model, "fraud": fraud_model},
        "silver": silver,
    }


# --- Local orchestrator (dev/testing): compose the stages -------------------
def run(cfg: PlatformConfig | None = None, *, persist: bool = True) -> dict:
    """Local end-to-end composition of the stages (generate -> bronze -> silver
    -> train -> gold). This is the dev/test harness and the training path; the
    individual stages above are what run standalone / in orchestration.
    """
    cfg = cfg or load_config()
    syn = cfg.synthetic
    dataset = generate_for_tenants(
        cfg.tenant_ids,
        subscribers=int(syn.get("subscribers_per_tenant", 200)),
        days=int(syn.get("days", 60)),
        fraud_rate=float(syn.get("fraud_rate", 0.05)),
        default_rate=float(syn.get("default_rate", 0.12)),
        seed=int(syn.get("seed", 42)),
    )
    frames = {e: getattr(dataset, e) for e in ENTITIES}

    to_bronze(cfg, frames, persist=persist)
    silver = to_silver(cfg, frames, persist=persist)
    trained = train(cfg, dataset=dataset, silver=silver, persist=persist)
    gold = to_gold(cfg, silver=silver, persist=persist, models=trained["models"])

    return {
        "tenants": cfg.tenant_ids,
        "counts": {
            "subscribers": int(len(silver["dim_subscriber"])),
            "usage_events": int(len(silver["fact_usage"])),
            "recharge_events": int(len(silver["fact_recharge"])),
        },
        "credit_metrics": trained["credit_metrics"],
        "fraud_metrics": trained["fraud_metrics"],
        "credit_scores": gold.get("credit_scores"),
        "fraud_scores": gold.get("fraud_scores"),
        "silver": silver,
    }
