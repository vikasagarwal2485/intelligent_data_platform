"""Tests for the synthetic data generator (plan 3.0)."""
from __future__ import annotations

from idp.synthetic.generator import generate, generate_for_tenants


def test_generates_all_entities(small_dataset):
    ds = small_dataset
    assert len(ds.parties) == 80
    assert len(ds.accounts) == 80
    assert len(ds.agreements) == 80
    assert not ds.products.empty
    assert not ds.voice_events.empty
    assert not ds.data_events.empty
    assert not ds.recharge_events.empty
    assert len(ds.labels) == 80


def test_referential_consistency(small_dataset):
    ds = small_dataset
    # every agreement's account exists
    assert set(ds.agreements["account_id"]).issubset(set(ds.accounts["id"]))
    # every usage event's service_id is a real agreement MSISDN
    msisdns = set(ds.agreements["msisdn"])
    assert set(ds.voice_events["service_id"]).issubset(msisdns)
    assert set(ds.recharge_events["service_id"]).issubset(msisdns)


def test_every_record_is_tenant_stamped(small_dataset):
    for name, df in small_dataset.tables().items():
        assert "tenant_id" in df.columns, name
        assert (df["tenant_id"] == "tenant_a").all(), name


def test_labels_have_both_classes(small_dataset):
    labels = small_dataset.labels
    assert labels["is_fraud"].nunique() == 2
    assert labels["will_default"].nunique() == 2
    assert (labels["true_credit_limit"] >= 5.0).all()
    assert (labels["true_credit_limit"] <= 500.0).all()


def test_deterministic_given_seed():
    a = generate("tenant_a", subscribers=20, days=10, seed=123)
    b = generate("tenant_a", subscribers=20, days=10, seed=123)
    assert a.voice_events.shape == b.voice_events.shape
    assert a.labels.equals(b.labels)


def test_different_tenants_differ():
    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=20, days=10, seed=1)
    a = ds.labels[ds.labels.tenant_id == "tenant_a"]["service_id"].tolist()
    b = ds.labels[ds.labels.tenant_id == "tenant_b"]["service_id"].tolist()
    assert set(a).isdisjoint(set(b))


def test_fraud_subscribers_show_elevated_behaviour(small_dataset):
    """SIM-box fraud subscribers should have higher voice volume on average."""
    ds = small_dataset
    fraud_simbox = ds.labels[ds.labels.fraud_type == "sim_box"]["service_id"]
    normal = ds.labels[ds.labels.fraud_type == "none"]["service_id"]
    vc = ds.voice_events.groupby("service_id").size()
    if len(fraud_simbox) and len(normal):
        assert vc.reindex(fraud_simbox).mean() > vc.reindex(normal).mean()
