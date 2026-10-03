"""Unit tests of the Spark Silver transforms on tiny hand-built inputs."""
from __future__ import annotations

import pytest

from idp.common.pii import tokenize_value
from idp.transforms import silver

SALTS = {"tenant_a": "salt-a"}


def _df(spark, rows, schema):
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def entities(spark):
    parties = _df(spark, [("tenant_a", "UUID(p1)", "1990-01-01", "en", True, True, "individual"),
                          ("tenant_a", "p2", None, "sv", False, False, "organization")],
                  "tenant_id string, id string, date_of_birth string, language string, "
                  "consent_marketing boolean, consent_analytics boolean, party_type string")
    customers = _df(spark, [
        ("tenant_a", "c1", "p1", "owner", "B2C", "approved", "2025-01-01T00:00:00.000Z", "a1"),
        ("tenant_a", "UUID(c2)", "UUID(p2)", "owner", "B2B", "approved", "2025-01-11T12:00:00.000Z", "a2"),
        ("tenant_a", "UUID(c2)", "UUID(p2)", "owner", "B2B", "approved", "2025-01-11T12:00:00.000Z", "a2"),
    ], "tenant_id string, id string, party_id string, role string, segment string, status string, "
       "valid_from string, account_id string")
    billing_accounts = _df(spark, [
        ("tenant_a", "a1", "salaried", "active", "2025-01-01T00:00:00.000Z", "p1", False),
        ("tenant_a", "a2", "company", "active", "2025-01-11T12:00:00.000Z", "UUID(p2)", True),
    ], "tenant_id string, id string, account_type string, lifecycle_status string, valid_from string, "
       "payer_party_id string, is_company boolean")
    agreements = _df(spark, [
        ("tenant_a", "g1", "subscription", "mobile", "2025-01-01", "4670001", "2400001", "c1", "UUID(a1)"),
        ("tenant_a", "g2", "device", "iot", "2025-01-01", "4670002", "2400002", "UUID(c2)", "a2"),
        ("tenant_a", "g2", "device", "iot", "2025-01-01", "4670002", "2400002", "UUID(c2)", "a2"),   # re-landed dup
    ], "tenant_id string, id string, agreement_type string, business_line string, valid_from string, "
       "msisdn string, imsi string, customer_id string, account_id string")
    products = _df(spark, [("tenant_a", "UUID(pr1)", "addon", "Data 1 GB", "addon", 4.99, "active", "g1"),
                           ("tenant_a", "pr2", "addon", "Voice 100", "subscription", 9.99, "active", "g1")],
                   "tenant_id string, id string, product_type string, name string, category string, "
                   "monthly_fee double, lifecycle_status string, agreement_id string")
    return parties, customers, billing_accounts, agreements, products


def test_dim_subscriber_one_row_per_subscriber_tokenized(spark, entities):
    rows = {r["service_id"]: r for r in silver.build_dim_subscriber(*entities, SALTS).collect()}
    t1, t2 = tokenize_value("4670001", "salt-a"), tokenize_value("4670002", "salt-a")
    assert set(rows) == {t1, t2}                                   # de-duplicated, raw MSISDN gone
    assert rows[t1]["num_products"] == 2 and rows[t2]["num_products"] == 0
    assert rows[t1]["customer_id"] == "c1" and rows[t2]["customer_id"] == "c2"   # UUID() stripped
    assert rows[t1]["consent_analytics"] is True and rows[t2]["language"] == "sv"  # the customer's party
    assert rows[t2]["has_device_agreement"] is True and rows[t2]["agreement_type"] == "device"
    assert rows[t1]["imsi"] == tokenize_value("2400001", "salt-a")


def test_customer_segment_and_tenure(spark, entities):
    # SCN-SLV-01.5: the customer's segment, and its tenure vs the latest customer
    # valid_from in the run (2025-01-11T12:00): 10.5 days -> floored to 10
    rows = {r["customer_id"]: r for r in silver.build_dim_subscriber(*entities, SALTS).collect()}
    assert (rows["c1"]["segment"], rows["c2"]["segment"]) == ("B2C", "B2B")
    assert rows["c1"]["customer_tenure_days"] == 10 and rows["c2"]["customer_tenure_days"] == 0


def test_dim_subscriber_column_contract(spark, entities):
    dim = silver.build_dim_subscriber(*entities, SALTS)
    assert dim.columns == silver.DIM_COLUMNS
    assert not {"party_id", "account_segment", "account_tenure_days"} & set(dim.columns)


def test_dimension_grains_and_keys(spark, entities):
    # SCN-SLV-01.3 / 01.4: one row per id; dim_subscriber's keys join the dimensions
    parties, customers, billing_accounts, agreements, products = entities
    cust = {r["customer_id"]: r for r in silver.build_dim_customer(customers, parties).collect()}
    assert set(cust) == {"c1", "c2"}
    assert (cust["c2"]["party_id"], cust["c2"]["party_type"], cust["c2"]["segment"]) == ("p2", "organization", "B2B")
    accts = {r["billing_account_id"]: r for r in silver.build_dim_billing_account(billing_accounts).collect()}
    assert accts["a2"]["payer_party_id"] == "p2" and accts["a2"]["is_company"] is True
    prods = {r["product_id"]: r for r in silver.build_dim_product(products).collect()}
    assert set(prods) == {"pr1", "pr2"} and prods["pr1"]["monthly_fee"] == 4.99
    dim = silver.build_dim_subscriber(*entities, SALTS).collect()
    assert {r["customer_id"] for r in dim} <= set(cust)
    assert {r["account_id"] for r in dim} <= set(accts)
    assert silver.build_dim_customer(customers, parties).columns == silver.DIM_CUSTOMER_COLUMNS
    assert silver.build_dim_billing_account(billing_accounts).columns == silver.DIM_BILLING_ACCOUNT_COLUMNS
    assert silver.build_dim_product(products).columns == silver.DIM_PRODUCT_COLUMNS


def test_fact_usage_unions_normalizes_and_tokenizes(spark):
    schema = ("tenant_id string, id string, service_id string, event_type string, started_at string, "
              "net_amount double, tax_amount double, qty double, other_party_number string, imei string")
    voice = _df(spark, [("tenant_a", "v1", "4670001", "voice", "2026-01-01T10:00:00.000Z", 1.0, 0.25, 60.0, "4670009", "35"),
                        ("tenant_a", "v1", "4670001", "voice", "2026-01-01T10:00:00.000Z", 1.0, 0.25, 60.0, "4670009", "35")],
                schema)
    data = _df(spark, [("tenant_a", "d1", "4670001", "data", "2026-01-01T11:00:00.000Z", None, None, 1e6, None, None)],
               schema)
    from pyspark.sql import functions as F
    out = silver.build_fact_usage(voice, data, SALTS)
    rows = {r["id"]: r for r in out.withColumn("utc_hour", F.hour("started_at")).collect()}
    assert set(rows) == {"v1", "d1"}                               # duplicate event dropped
    assert rows["v1"]["gross_amount"] == 1.25                      # net + tax
    assert rows["d1"]["gross_amount"] == 0.0 and rows["d1"]["net_amount"] == 0.0
    assert rows["v1"]["service_id"] == tokenize_value("4670001", "salt-a")
    assert rows["v1"]["other_party_number"] == tokenize_value("4670009", "salt-a")
    assert rows["d1"]["other_party_number"] is None
    assert rows["v1"]["utc_hour"] == 10                            # parsed as UTC


def test_fact_recharge(spark):
    rech = _df(spark, [("tenant_a", "r1", "4670001", "2026-01-02T00:00:00.000Z", None, "successful")],
               "tenant_id string, id string, service_id string, created_at string, amount double, status string")
    (row,) = silver.build_fact_recharge(rech, SALTS).collect()
    assert row["amount"] == 0.0 and row["service_id"] == tokenize_value("4670001", "salt-a")


def test_fact_fee_charge_enriched_from_products(spark, entities):
    *_, products = entities
    fees = _df(spark, [("tenant_a", "f1", "UUID(pr1)", "recurring", 9.99, "2026-01-01T00:00:00.000Z"),
                       ("tenant_a", "f2", "missing", "one_time", None, "2026-01-02T00:00:00.000Z")],
               "tenant_id string, id string, product_id string, charge_type string, amount double, charged_at string")
    out = silver.build_fact_fee_charge(fees, products)
    assert out.columns == ["tenant_id", "id", "product_id", "charge_type", "amount", "charged_at",
                           "product_name", "category", "agreement_id"]
    rows = {r["id"]: r for r in out.collect()}
    assert (rows["f1"]["product_id"], rows["f1"]["product_name"], rows["f1"]["agreement_id"]) == \
        ("pr1", "Data 1 GB", "g1")
    assert rows["f2"]["product_name"] is None and rows["f2"]["amount"] == 0.0
    assert "service_id" not in out.columns                         # no PII on the charge


def test_curate_requires_core_entities(spark, entities):
    parties, customers, billing_accounts, _, _ = entities
    with pytest.raises(RuntimeError, match="Populate Bronze first"):
        silver.curate({"parties": parties, "customers": customers, "billing_accounts": billing_accounts}, SALTS)


def test_uuid_wrapped_relationship_ids_join_their_target(spark, entities):
    # SCN-SLV-02.2 — agreement g1 references its billing account as "UUID(a1)", as
    # some operator feeds send ids; Silver strips the wrapper so it joins a1.
    rows = {r["service_id"]: r for r in silver.build_dim_subscriber(*entities, SALTS).collect()}
    row = rows[tokenize_value("4670001", "salt-a")]
    assert row["account_id"] == "a1"
    assert (row["account_type"], row["segment"]) == ("salaried", "B2C")   # a1's type, c1's segment
