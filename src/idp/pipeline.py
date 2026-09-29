"""Medallion pipeline stages (plan sections 3-6).

Each stage is an INDEPENDENT step that reads its input layer and writes its
output layer, so the concerns stay separated (no consumer->silver->gold chaining):

    to_bronze      : raw frames              -> Bronze   (s3://idp-bronze / local)
    to_silver      : Bronze  -> canonical    -> Silver   (s3://idp-silver / local)
    to_gold        : Silver  -> features     -> Gold     (s3://idp-gold   / local; features ONLY)
    to_scores      : Gold features + models  -> Gold score tables (inference)
    publish_serving: Gold    -> TimescaleDB serving copy
    to_labels      : ground-truth outcomes   -> labels store (s3://idp-ml/labels)
    train          : Gold features + labels  -> models (offline ML; labels not in the lake)

Stages exchange data ONLY through the lake (object storage, or a local lake
directory): each reads its input layer and writes its output layer, locally and in
the cluster alike. Silver and Gold run on Spark — the single transformation engine
(``idp.transforms``, REQ-ORC-05); pandas is used for ML (labels, training,
scoring), the Bronze landing and serving. The `idp` CLI exposes the stages as
`consume` (Bronze), `silver`, `gold`, `score`, `publish`, `labels`, `train`; `run`
composes them locally for dev/testing.
"""
from __future__ import annotations

import shutil
import tempfile
from dataclasses import replace
from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split

from idp.common import pii, storage
from idp.common.config import PlatformConfig, load_config
from idp.ml.credit_model import CreditLimitModel
from idp.ml.fraud_model import FraudModel
from idp.synthetic.generator import ENTITIES, SyntheticDataset, generate, generate_for_tenants
SILVER_TABLES = ("dim_subscriber", "fact_usage", "fact_recharge", "fact_fee_charge")


def _salts(cfg: PlatformConfig) -> dict[str, str]:
    return {t.id: t.pii_salt for t in cfg.tenants}


def _read_layer(cfg: PlatformConfig, layer: str, tables) -> dict[str, pd.DataFrame]:
    """Read lake tables of a layer into pandas via the Iceberg catalog (the ML /
    serving boundary); tables never written are skipped."""
    from idp.common import catalog

    out: dict[str, pd.DataFrame] = {}
    for t in tables:
        if catalog.table_exists(cfg, layer, t):
            out[t] = catalog.read_table(cfg, layer, t)
    return out


# --- Stage 1: Bronze --------------------------------------------------------
BRONZE_PARTITIONS = ("tenant_id", "ingest_date")


def to_bronze(cfg: PlatformConfig, frames: dict[str, pd.DataFrame], *, persist: bool = True) -> dict:
    """Append raw per-entity frames to Iceberg Bronze (REQ-BRZ-01).

    Each entity becomes one append (one snapshot) to ``bronze.<entity>``,
    partitioned by ``(tenant_id, ingest_date)``; every record carries the batch's
    landing time ``_ingested_at`` (UTC) and ``ingest_date``. Bronze is an
    append-only raw log: nothing already landed is rewritten or removed (Silver
    de-duplicates on natural keys).
    """
    from idp.common import catalog

    landed = [n for n, d in frames.items() if d is not None and not d.empty]
    if persist and landed:
        ingested_at = catalog.now_utc()
        for name in landed:
            df = frames[name].assign(_ingested_at=pd.Timestamp(ingested_at),
                                     ingest_date=ingested_at.date())
            catalog.append_table(cfg, "bronze", name, df, partition_by=BRONZE_PARTITIONS)
    return {"bronze_tables": landed}


# --- Stage 2: Silver (Spark) -----------------------------------------------
def _table_count(spark, cfg: PlatformConfig, layer: str, table: str) -> int:
    """Row count of a lake table (answered from Iceberg file metadata)."""
    from idp.transforms import lake
    df = lake.read_table(spark, cfg, layer, table)
    return 0 if df is None else int(df.count())


def to_silver(cfg: PlatformConfig, *, full_refresh: bool = False) -> dict:
    """Iceberg Bronze -> canonical Iceberg Silver, incrementally, on Spark.

    ``dim_subscriber`` is rebuilt from the Bronze reference tables; the event facts
    process only the Bronze appended since the last run and merge it on
    ``(tenant_id, id)`` (REQ-SLV-04/06/07). ``full_refresh`` drops the fact
    checkpoints and rebuilds every Silver table from all of Bronze (REQ-SLV-08).
    Returns ``{"silver_tables": {table: rows}, "processed": {fact: new_bronze_rows}}``;
    no table is materialized in driver memory.
    """
    from idp.transforms import incremental
    from idp.transforms.session import get_spark

    spark = get_spark(cfg, app_name="idp-silver", stage="silver")
    salts = _salts(cfg)
    incremental.build_dim(spark, cfg, salts, full_refresh=full_refresh)
    if full_refresh:
        incremental.reset_facts(spark, cfg)
    processed = incremental.process_facts(spark, cfg, salts)
    tables = ["dim_subscriber", *[f for f in incremental.FACTS if _exists(cfg, "silver", f)]]
    return {"silver_tables": {name: _table_count(spark, cfg, "silver", name) for name in tables},
            "processed": processed}


def _exists(cfg: PlatformConfig, layer: str, table: str) -> bool:
    from idp.common import catalog
    return catalog.table_exists(cfg, layer, table)


# --- Stage 3: Gold (Spark) --------------------------------------------------
GOLD_FEATURE_TABLES = ("credit_features", "fraud_features")


def to_gold(cfg: PlatformConfig) -> dict:
    """Silver (lake) -> Gold **feature tables only** (lake), on Spark.

    Always reads Silver from the lake (REQ-GLD-05). Scoring is a SEPARATE stage
    (``to_scores`` / ``idp score``) so feature-build and inference are decoupled:
    Gold never needs a model to exist, and training/scoring run on their own cadence.
    """
    from idp.transforms import features, lake
    from idp.transforms.session import get_spark

    spark = get_spark(cfg, app_name="idp-gold", stage="gold")
    silver = {t: lake.read_table(spark, cfg, "silver", t) for t in SILVER_TABLES}
    silver = {t: df for t, df in silver.items() if df is not None}
    dim = silver.get("dim_subscriber")
    if dim is None or dim.isEmpty():
        raise RuntimeError("Silver is empty. Run the Silver stage ('idp silver') first.")
    missing = [t for t in ("fact_usage", "fact_recharge") if t not in silver]
    if missing:
        raise RuntimeError(f"Silver is missing {missing}. Run the Silver stage ('idp silver') first.")

    windows = tuple(int(w) for w in cfg.features.get("windows", [30, 90]))
    lake.overwrite_tenants(features.build_credit_features(silver, windows), cfg, "gold", "credit_features")
    lake.overwrite_tenants(features.build_fraud_features(silver, windows), cfg, "gold", "fraud_features")
    return {
        "gold_tables": list(GOLD_FEATURE_TABLES),
        "counts": {"subscribers": _table_count(spark, cfg, "gold", "credit_features")},
    }


# --- Stage 3b: Scoring / inference (Gold features -> scores) ----------------
def to_scores(cfg: PlatformConfig, *, models: dict | None = None, persist: bool = True) -> dict:
    """Gold features -> ML scores, distributed on Spark (REQ-ML-07).

    Reads the Gold feature tables from the lake as Spark DataFrames and the trained
    models from the model store (or ``models``, for tests), broadcasts the models,
    and scores each partition (``transforms.scoring``). The run is appended to
    ``credit_scores`` / ``fraud_scores``. No process holds a whole table
    (REQ-ORC-05). Graceful: a model absent from the store means that score table is
    skipped. Returns ``score_tables``, their row ``counts``, and the scored data as
    lazy Spark DataFrames under each table name.
    """
    from pyspark.sql import functions as F

    from idp.transforms import lake
    from idp.transforms.scoring import load_scoring_models, score_features
    from idp.transforms.session import get_spark

    spark = get_spark(cfg, app_name="idp-score", stage="score")
    features = {t: lake.read_table(spark, cfg, "gold", t) for t in GOLD_FEATURE_TABLES}
    credit = features.get("credit_features")
    if credit is None or credit.isEmpty():
        raise RuntimeError("Gold features are missing. Run the Gold stage ('idp gold') first.")

    # One UTC timestamp per `idp score` run, so analytics can build 7/15/30-day
    # windows over the accumulating score history.
    scored_at = pd.Timestamp.now(tz="UTC").to_pydatetime()
    outputs = score_features(spark, load_scoring_models(cfg, models), features, scored_at)

    counts: dict[str, int] = {}
    for name, df in outputs.items():
        if persist:
            # Append: score tables are the run-by-run history (one snapshot per run).
            lake.append(df, cfg, "gold", name, partition_by=("tenant_id", F.days("scored_at")))
        counts[name] = int(df.count())
    return {"score_tables": list(outputs), "counts": counts, **outputs}


# --- Stage 4: Serving (Gold -> TimescaleDB) ---------------------------------
GOLD_SERVING_TABLES = ("credit_scores", "fraud_scores", "credit_features", "fraud_features")
SCORE_TABLES = ("credit_scores", "fraud_scores")


def latest_score_run(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only each tenant's latest ``scored_at`` run of a score table.

    Score tables accumulate one snapshot per `idp score` run; serving wants just
    the current one. Frames without ``scored_at`` (legacy) are returned as-is.
    """
    if df is None or df.empty or "scored_at" not in df.columns or df["scored_at"].isna().all():
        return df
    ts = pd.to_datetime(df["scored_at"], utc=True, errors="coerce")
    latest = ts.groupby(df["tenant_id"]).transform("max")
    return df[ts == latest].reset_index(drop=True)


def latest_score_run_spark(df):
    """Spark form of ``latest_score_run``: each tenant's latest ``scored_at`` rows."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if "scored_at" not in df.columns:
        return df
    latest = F.max("scored_at").over(Window.partitionBy("tenant_id"))
    return df.withColumn("_latest", latest).where(F.col("scored_at") == F.col("_latest")).drop("_latest")


def publish_serving(cfg: PlatformConfig, gold: dict | None = None) -> dict:
    """Publish Gold tables to the TimescaleDB serving layer (REQ-SRV-01).

    Reads the Gold tables with Spark, reduces the score tables to each tenant's
    latest run, loads each into a staging table in parallel over JDBC, and swaps it
    in atomically, uniquely indexed by (tenant_id, service_id). No table is
    collected on the driver (REQ-ORC-05). ``gold`` (pandas frames) is a test/dev
    hook that publishes those frames through the same swap instead.
    """
    from idp.serving import timescale

    if gold is not None:
        frames = {name: latest_score_run(df) if name in SCORE_TABLES else df
                  for name, df in gold.items()}
        if not frames:
            raise RuntimeError("No Gold tables found to publish. Run the Gold stage ('idp gold') first.")
        return {"published": timescale.publish_gold(cfg, frames), "target": cfg.timescale.get("host")}

    from idp.transforms import lake
    from idp.transforms.session import get_spark

    spark = get_spark(cfg, app_name="idp-publish", stage="publish")
    frames = {}
    for name in GOLD_SERVING_TABLES:
        df = lake.read_table(spark, cfg, "gold", name)
        if df is not None:
            frames[name] = latest_score_run_spark(df) if name in SCORE_TABLES else df
    if not frames:
        raise RuntimeError("No Gold tables found to publish. Run the Gold stage ('idp gold') first.")
    return {"published": timescale.publish_spark(cfg, frames), "target": cfg.timescale.get("host")}


# --- Labels/outcomes store (offline training input; kept out of the lake) ----
def _tokenize_labels(labels: pd.DataFrame, salts: dict[str, str]) -> pd.DataFrame:
    parts = []
    for tenant_id, part in labels.groupby("tenant_id"):
        parts.append(pii.tokenize_columns(part, salts.get(str(tenant_id), ""), columns=("service_id",)))
    return pd.concat(parts, ignore_index=True)


def _gen_dataset(cfg: PlatformConfig) -> SyntheticDataset:
    syn = cfg.synthetic
    return generate_for_tenants(
        cfg.tenant_ids,
        subscribers=int(syn.get("subscribers_per_tenant", 200)),
        days=int(syn.get("days", 60)),
        fraud_rate=float(syn.get("fraud_rate", 0.05)),
        default_rate=float(syn.get("default_rate", 0.12)),
        seed=int(syn.get("seed", 42)),
    )


def to_labels(cfg: PlatformConfig, *, dataset: SyntheticDataset | None = None,
              persist: bool = True) -> pd.DataFrame:
    """Persist ground-truth outcomes (labels) to the labels store (`s3://<prefix>-ml/labels`).

    Simulates the business's observed-outcomes table (`is_fraud`/`will_default`/
    `true_credit_limit`), tokenized on `service_id` so the offline train stage can
    join them to Gold features without exposing raw ids. Written OUTSIDE the Gold
    serving path so labels never leak into features/serving.
    """
    cfg = cfg or load_config()
    if dataset is not None:
        labels = _tokenize_labels(dataset.labels, _salts(cfg))
    else:
        # Generate ONE tenant at a time and keep only the labels (drop the heavy
        # event tables) so peak memory stays small — mirrors the produce job.
        # Uses the same synthetic params as produce (subscribers/days/seed), which
        # is REQUIRED: the labels' tokenized service_id must match the Gold features
        # produced from the same generation, or the training join is empty.
        syn = cfg.synthetic
        parts = []
        for tenant_id in cfg.tenant_ids:
            ds = generate(
                tenant_id,
                subscribers=int(syn.get("subscribers_per_tenant", 200)),
                days=int(syn.get("days", 60)),
                fraud_rate=float(syn.get("fraud_rate", 0.05)),
                default_rate=float(syn.get("default_rate", 0.12)),
                seed=int(syn.get("seed", 42)),
            )
            parts.append(ds.labels)
            del ds
        labels = _tokenize_labels(pd.concat(parts, ignore_index=True), _salts(cfg))
    if persist:
        storage.write_parquet(labels, storage.labels_uri(cfg), cfg, partition_cols=["tenant_id"])
    return labels


def _read_labels_spark(spark, cfg: PlatformConfig):
    """The labels store as a Spark DataFrame (None when absent), one row per
    subscriber: re-seeding appends identical copies, so duplicates are dropped."""
    uri = storage.labels_uri(cfg)
    try:
        df = spark.read.parquet(uri)
    except Exception:  # noqa: BLE001 - absent labels store
        return None
    from pyspark.sql import functions as F
    return df.withColumn("tenant_id", F.col("tenant_id").cast("string")).dropDuplicates(["tenant_id", "service_id"])


def _read_labels(cfg: PlatformConfig) -> pd.DataFrame:
    try:
        df = storage.read_parquet(storage.labels_uri(cfg), cfg)
    except Exception:  # noqa: BLE001 - absent labels store -> empty
        return pd.DataFrame()
    if "tenant_id" in df.columns:
        df["tenant_id"] = df["tenant_id"].astype(str)
    # Partitioned-parquet writes append, so re-seeding stacks copies — keep the
    # latest label per subscriber so the training join can't fan out duplicates.
    if {"tenant_id", "service_id"} <= set(df.columns):
        df = df.drop_duplicates(subset=["tenant_id", "service_id"], keep="last").reset_index(drop=True)
    return df


# --- Offline ML training (separate concern; needs labels) -------------------
def _safe_split(df: pd.DataFrame, target: str):
    y = df[target]
    counts = y.value_counts()
    if len(df) < 8 or y.nunique() < 2:
        return df, df
    stratify = y if counts.min() >= 2 else None
    return train_test_split(df, test_size=0.25, random_state=42, stratify=stratify)


def train(cfg: PlatformConfig, *, persist: bool = True) -> dict:
    """Offline model training on the lake (REQ-ML-03/06).

    Reads historical Gold **features** + the **labels store** with Spark (a local
    lake in dev/tests), joins them, bounds each model's training set to
    ``ml.train_max_rows`` with a stratified per-(tenant, label) sample (REQ-ML-09),
    fits the credit and fraud models on that sample, evaluates out-of-sample, and
    saves them to the model store (REQ-ML-04; ``registry`` in the result reports each
    model's version and whether it passed the gate and became ``champion``). The metrics record ``sampled_rows`` and
    ``sample_seed``. Never builds features from Silver and never regenerates
    synthetic data.
    """
    from idp.transforms import lake
    from idp.transforms.sampling import stratified_cap
    from idp.transforms.session import get_spark

    cfg = cfg or load_config()
    spark = get_spark(cfg, app_name="idp-train", stage="train")
    credit_feats = lake.read_table(spark, cfg, "gold", "credit_features")
    fraud_feats = lake.read_table(spark, cfg, "gold", "fraud_features")
    labels = _read_labels_spark(spark, cfg)
    if credit_feats is None or credit_feats.isEmpty() or fraud_feats is None or fraud_feats.isEmpty() \
            or labels is None or labels.isEmpty():
        raise RuntimeError(
            "Training needs Gold features and a populated labels store. Run "
            "'idp gold' and seed labels ('idp labels' / make labels) first."
        )

    # Join in Spark, then take at most ml.train_max_rows per model: a stratified
    # sample per (tenant, label class) with a fixed seed (REQ-ML-09). Only the bounded
    # sample reaches pandas for fitting.
    cap, seed = int(cfg.ml["train_max_rows"]), int(cfg.ml["sample_seed"])
    keys = ["tenant_id", "service_id"]
    credit_sdf, credit_rows = stratified_cap(credit_feats.join(labels, on=keys),
                                             ["tenant_id", "will_default"], cap, seed)
    fraud_sdf, fraud_rows = stratified_cap(fraud_feats.join(labels, on=keys),
                                           ["tenant_id", "is_fraud"], cap, seed)
    credit_df = credit_sdf.toPandas().sort_values(keys).reset_index(drop=True)
    fraud_df = fraud_sdf.toPandas().sort_values(keys).reset_index(drop=True)

    if credit_df.empty:
        raise RuntimeError(
            "Training set is empty: labels did not join any Gold features "
            f"(gold rows={credit_feats.count()}, label rows={labels.count()}). The labels "
            "must be generated with the SAME synthetic params (subscribers/days/seed) "
            "as the data that produced Gold — otherwise the tokenized service_ids "
            "differ. Re-seed labels ('idp labels'/make labels) with matching "
            "IDP_SUBSCRIBERS/IDP_DAYS, or regenerate the lake end-to-end."
        )

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
    credit_metrics.update(sampled_rows=credit_rows, sample_seed=seed)
    fraud_metrics.update(sampled_rows=fraud_rows, sample_seed=seed)

    registry: dict[str, dict] = {}
    if persist:
        # The model store (REQ-ML-04): MLflow registers both versions and moves
        # `champion` only to those that pass the gate; the object-store fallback just
        # replaces the files.
        from idp.ml.registry import model_store
        params = {"train_max_rows": cap, "sample_seed": seed,
                  "credit_rows": credit_rows, "fraud_rows": fraud_rows,
                  "credit_base_multiplier": credit_model.base_multiplier,
                  "credit_min_limit": credit_model.min_limit, "credit_max_limit": credit_model.max_limit,
                  "fraud_contamination": fraud_model.contamination}
        saved = model_store(cfg).save({"credit": (credit_model, credit_metrics),
                                       "fraud": (fraud_model, fraud_metrics)}, params)
        registry = {name: {"version": r.version, "promoted": r.promoted,
                           "gate": "passed" if r.gate.passed else "failed", "gate_reason": r.gate.reason}
                    for name, r in saved.items()}

    return {
        "credit_metrics": credit_metrics,
        "fraud_metrics": fraud_metrics,
        "registry": registry,
        "models": {"credit": credit_model, "fraud": fraud_model},
    }


# --- Local orchestrator (dev/testing): compose the stages -------------------
def run(cfg: PlatformConfig | None = None, *, persist: bool = True) -> dict:
    """Local end-to-end composition through the lake: generate -> bronze ->
    silver -> labels -> gold (features) -> train -> score (REQ-ORC-03).

    Every stage reads what the previous one wrote, exactly as the separate
    CLI stages / DAG tasks do. ``persist=False`` runs against a temporary local
    lake that is removed afterwards (not supported with S3 enabled).
    """
    cfg = cfg or load_config()
    tmp = None
    if not persist:
        if cfg.s3.get("enabled"):
            raise RuntimeError("run(persist=False) uses a temporary LOCAL lake; unset IDP_S3_ENDPOINT.")
        tmp = Path(tempfile.mkdtemp(prefix="idp-lake-"))
        cfg = replace(cfg, storage=replace(
            cfg.storage, root=str(tmp), landing=str(tmp / "landing"), bronze=str(tmp / "bronze"),
            silver=str(tmp / "silver"), gold=str(tmp / "gold"), ml_artifacts=str(tmp / "ml")))
    try:
        dataset = _gen_dataset(cfg)
        to_bronze(cfg, {e: getattr(dataset, e) for e in ENTITIES})
        silver = to_silver(cfg)
        to_labels(cfg, dataset=dataset)                    # seed the outcomes store
        to_gold(cfg)                                       # features only
        trained = train(cfg)
        scores = to_scores(cfg, models=trained["models"])  # inference on Gold features
        # local dev summary: materialize the (small) score frames before a temporary
        # lake is removed below; the stage itself never does this (REQ-ORC-05)
        scores = {k: (v.toPandas() if hasattr(v, "toPandas") else v) for k, v in scores.items()}
        counts = silver["silver_tables"]
        return {
            "tenants": cfg.tenant_ids,
            "counts": {
                "subscribers": counts.get("dim_subscriber", 0),
                "usage_events": counts.get("fact_usage", 0),
                "recharge_events": counts.get("fact_recharge", 0),
            },
            "credit_metrics": trained["credit_metrics"],
            "fraud_metrics": trained["fraud_metrics"],
            "credit_scores": scores.get("credit_scores"),
            "fraud_scores": scores.get("fraud_scores"),
        }
    finally:
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)
