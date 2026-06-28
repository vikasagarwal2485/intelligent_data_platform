"""Shared pytest fixtures."""
from __future__ import annotations

import pytest

import pandas as pd

from idp.common import pii
from idp.curation.silver import curate
from idp.features.credit_features import build_credit_features
from idp.features.fraud_features import build_fraud_features
from idp.synthetic.generator import generate, generate_for_tenants

SALTS = {"tenant_a": "salt-a", "tenant_b": "salt-b"}


def _tokenize_labels(labels: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for tenant_id, part in labels.groupby("tenant_id"):
        parts.append(pii.tokenize_columns(part, SALTS[str(tenant_id)], columns=("service_id",)))
    return pd.concat(parts, ignore_index=True)


@pytest.fixture(scope="session")
def small_dataset():
    """A small single-tenant dataset (fast)."""
    return generate("tenant_a", subscribers=80, days=30, fraud_rate=0.2, seed=7)


@pytest.fixture(scope="session")
def multi_tenant_dataset():
    """Two-tenant dataset for isolation tests."""
    return generate_for_tenants(
        ["tenant_a", "tenant_b"], subscribers=80, days=30, fraud_rate=0.2, seed=7
    )


@pytest.fixture(scope="session")
def bronze_frames(multi_tenant_dataset):
    ds = multi_tenant_dataset
    return {
        "parties": ds.parties, "accounts": ds.accounts, "agreements": ds.agreements,
        "products": ds.products, "voice_events": ds.voice_events,
        "data_events": ds.data_events, "recharge_events": ds.recharge_events,
    }


@pytest.fixture(scope="session")
def silver(bronze_frames):
    return curate(bronze_frames, SALTS)


@pytest.fixture(scope="session")
def labels(multi_tenant_dataset):
    return _tokenize_labels(multi_tenant_dataset.labels)


@pytest.fixture(scope="session")
def credit_labeled(silver, labels):
    feats = build_credit_features(silver)
    return feats.merge(labels, on=["tenant_id", "service_id"], how="inner")


@pytest.fixture(scope="session")
def fraud_labeled(silver, labels):
    feats = build_fraud_features(silver)
    return feats.merge(labels, on=["tenant_id", "service_id"], how="inner")
