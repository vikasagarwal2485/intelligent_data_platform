"""Tests for Silver curation (plan 3.3)."""
from __future__ import annotations

from idp.common.tenancy import TENANT_COLUMN


def test_subscriber_dim_one_row_per_subscriber(silver):
    dim = silver["dim_subscriber"]
    assert dim.duplicated(subset=["tenant_id", "service_id"]).sum() == 0
    assert {"account_type", "customer_tenure_days", "num_products"}.issubset(dim.columns)


def test_service_id_is_tokenized(silver):
    dim = silver["dim_subscriber"]
    assert dim["service_id"].str.startswith("tok_").all()


def test_fact_usage_gross_is_net_plus_tax(silver):
    fu = silver["fact_usage"]
    sample = fu.head(200)
    assert ((sample["gross_amount"] - (sample["net_amount"] + sample["tax_amount"])).abs() < 1e-6).all()


def test_all_silver_tables_tenant_stamped(silver):
    for name, df in silver.items():
        assert TENANT_COLUMN in df.columns, name


def test_tokens_are_tenant_specific(silver):
    """Same raw MSISDN under different salts must not collide across tenants."""
    dim = silver["dim_subscriber"]
    a = set(dim[dim.tenant_id == "tenant_a"]["service_id"])
    b = set(dim[dim.tenant_id == "tenant_b"]["service_id"])
    assert a.isdisjoint(b)


def test_usage_and_dim_join_on_token(silver):
    """Tokenized service_id must be consistent so features can join facts to dim."""
    dim = set(silver["dim_subscriber"]["service_id"])
    usage = set(silver["fact_usage"]["service_id"])
    # the vast majority of usage subjects exist in the dimension
    assert len(usage & dim) / max(1, len(usage)) > 0.9
