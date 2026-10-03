"""Analytics consent enforced on the ML/serving path (REQ-FND-05).

Non-consenting subscribers are dropped when Gold features are built, so they never
reach training, scoring, the serving store or score analytics.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine

from conftest import rewrite_table
from golden_data import golden_cfg
from idp.ingestion.raw import raw_events
from idp.common import storage
from idp.common.pii import ConsentError
from idp.pipeline import (
    ENTITIES, publish_serving, to_bronze, to_gold, to_labels, to_scores, to_silver, train,
)
from idp.serving import timescale
from idp.synthetic.generator import generate_for_tenants

KEY = ["tenant_id", "service_id"]


def _read(cfg, layer, table) -> pd.DataFrame:
    from idp.common import catalog
    return catalog.read_table(cfg, layer, table)


def _keys(df: pd.DataFrame) -> set[tuple[str, str]]:
    return set(map(tuple, df[KEY].astype(str).values))


def _rewrite_dim(cfg, dim: pd.DataFrame) -> None:
    """Replace Silver dim_subscriber (tenant partitions) with ``dim``."""
    storage.write_parquet(dim, f"{storage.lake_uri(cfg, 'silver')}/dim_subscriber", cfg,
                          partition_cols=["tenant_id"])


@pytest.fixture()
def lake(spark, tmp_path):
    cfg = golden_cfg(tmp_path)
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=60, days=30,
                              fraud_rate=0.2, default_rate=0.2, seed=7)
    to_bronze(cfg, raw_events(ds))
    to_silver(cfg)
    to_labels(cfg, dataset=ds)
    return cfg


def _serving(cfg, tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path}/serving.db")
    monkeypatch.setattr(timescale, "get_engine", lambda _cfg: engine)
    cfg.raw["timescale"] = {"schema": None}
    return engine


def test_gold_without_consent_column_fails_and_writes_nothing(lake):
    # SCN-FND-05.4
    rewrite_table(lake, "silver", "dim_subscriber", _read(lake, "silver", "dim_subscriber"),
                  drop_columns=("consent_analytics",))
    with pytest.raises(ConsentError, match="consent_analytics"):
        to_gold(lake)
    from idp.common import catalog
    assert not catalog.load_catalog(lake).list_tables("gold")


def test_non_consenting_never_trained_scored_or_served(lake, tmp_path, monkeypatch):
    # SCN-FND-05.1 / SCN-FND-05.2
    from idp.ml.credit_model import CreditLimitModel
    dim = _read(lake, "silver", "dim_subscriber")
    refused = _keys(dim[~dim["consent_analytics"].fillna(False).astype(bool)])
    consenting = _keys(dim) - refused
    assert refused, "the synthetic data should contain non-consenting subscribers"

    to_gold(lake)
    for table in ("credit_features", "fraud_features"):
        gold = _read(lake, "gold", table)
        assert _keys(gold) == consenting and not gold.duplicated(KEY).any()

    seen: list[pd.DataFrame] = []
    real_fit = CreditLimitModel.fit
    monkeypatch.setattr(CreditLimitModel, "fit",
                        lambda self, X, y: seen.append(X) or real_fit(self, X, y))
    models = train(lake, persist=False)["models"]
    assert seen and not (_keys(seen[0]) & refused)             # training set

    scores = to_scores(lake, models=models)
    for table in ("credit_scores", "fraud_scores"):
        assert not (_keys(scores[table].toPandas()) & refused)       # test-side materialization

    engine = _serving(lake, tmp_path, monkeypatch)
    published = publish_serving(lake)["published"]
    for table in published:
        assert not (_keys(pd.read_sql(f'SELECT * FROM "{table}"', engine)) & refused), table


def test_revocation_takes_effect_on_refresh(lake, tmp_path, monkeypatch):
    # SCN-FND-05.5
    to_gold(lake)
    models = train(lake, persist=False)["models"]
    to_scores(lake, models=models)
    dim = _read(lake, "silver", "dim_subscriber")
    target = dim[dim["consent_analytics"].fillna(False).astype(bool)].iloc[0]
    key = (str(target["tenant_id"]), str(target["service_id"]))
    assert key in _keys(_read(lake, "gold", "credit_features"))

    dim.loc[(dim["tenant_id"] == key[0]) & (dim["service_id"] == key[1]), "consent_analytics"] = False
    rewrite_table(lake, "silver", "dim_subscriber", dim)      # the subscriber revokes consent
    to_gold(lake)
    to_scores(lake, models=models)
    engine = _serving(lake, tmp_path, monkeypatch)
    publish_serving(lake)

    assert key not in _keys(_read(lake, "gold", "credit_features"))
    assert key not in _keys(_read(lake, "gold", "fraud_features"))
    scores = _read(lake, "gold", "credit_scores")
    latest = scores[pd.to_datetime(scores["scored_at"], utc=True)
                    == pd.to_datetime(scores["scored_at"], utc=True).max()]
    assert key not in _keys(latest)
    assert key not in _keys(pd.read_sql('SELECT * FROM "credit_scores"', engine))
