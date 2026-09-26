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
    parties = _df(spark, [("tenant_a", "UUID(p1)", "1990-01-01", "en", True, True),
                          ("tenant_a", "p2", "1980-01-01", "sv", False, False)],
                  "tenant_id string, id string, date_of_birth string, language string, "
                  "consent_marketing boolean, consent_analytics boolean")
    accounts = _df(spark, [("tenant_a", "a1", "salaried", "B2C", "2025-01-01T00:00:00.000Z", "p1", False),
                           ("tenant_a", "a2", "company", "B2B", "2025-01-11T12:00:00.000Z", "UUID(p2)", True)],
                   "tenant_id string, id string, account_type string, account_segment string, "
                   "valid_from string, party_id string, is_company boolean")
    agreements = _df(spark, [
        ("tenant_a", "g1", "subscription", "mobile", "2025-01-01", "4670001", "2400001", "UUID(a1)"),
        ("tenant_a", "g2", "device", "iot", "2025-01-01", "4670002", "2400002", "a2"),
        ("tenant_a", "g2", "device", "iot", "2025-01-01", "4670002", "2400002", "a2"),   # re-landed dup
    ], "tenant_id string, id string, agreement_type string, business_line string, valid_from string, "
       "msisdn string, imsi string, account_id string")
    products = _df(spark, [("tenant_a", "UUID(pr1)", "Data 1 GB", "addon", "g1"),
                           ("tenant_a", "pr2", "Voice 100", "subscription", "g1")],
                   "tenant_id string, id string, name string, category string, agreement_id string")
    return parties, accounts, agreements, products


def test_dim_subscriber_one_row_per_subscriber_tokenized(spark, entities):
    parties, accounts, agreements, products = entities
    rows = {r["service_id"]: r for r in
            silver.build_dim_subscriber(parties, accounts, agreements, products, SALTS).collect()}
    t1, t2 = tokenize_value("4670001", "salt-a"), tokenize_value("4670002", "salt-a")
    assert set(rows) == {t1, t2}                                   # de-duplicated, raw MSISDN gone
    assert rows[t1]["num_products"] == 2 and rows[t2]["num_products"] == 0
    assert rows[t1]["party_id"] == "p1" and rows[t1]["consent_analytics"] is True  # UUID() stripped
    assert rows[t2]["has_device_agreement"] is True and rows[t2]["agreement_type"] == "device"
    # tenure vs the latest account valid_from (2025-01-11T12:00): 10.5 days -> floored to 10
    assert rows[t1]["account_tenure_days"] == 10 and rows[t2]["account_tenure_days"] == 0
    assert rows[t1]["imsi"] == tokenize_value("2400001", "salt-a")


def test_dim_subscriber_column_contract(spark, entities):
    dim = silver.build_dim_subscriber(*entities, SALTS)
    assert dim.columns == silver.DIM_COLUMNS


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
    parties, accounts, _, _ = entities
    with pytest.raises(RuntimeError, match="Populate Bronze first"):
        silver.curate({"parties": parties, "accounts": accounts}, SALTS)
