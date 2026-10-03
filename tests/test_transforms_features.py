"""Unit tests of the Spark Gold feature transforms on tiny hand-built Silver."""
from __future__ import annotations

import pytest

from idp.common.feature_contract import CREDIT_FEATURES, FRAUD_FEATURES
from idp.transforms import features

DIM_SCHEMA = ("tenant_id string, service_id string, agreement_id string, customer_tenure_days long, "
              "num_products long, has_device_agreement boolean, is_company boolean, "
              "consent_analytics boolean")
RECH_SCHEMA = ("tenant_id string, id string, service_id string, created_at timestamp, amount double, "
               "status string, payment_method string")
USAGE_SCHEMA = ("tenant_id string, id string, service_id string, event_type string, started_at timestamp, "
                "gross_amount double, qty double, is_roaming boolean, traffic_case string, "
                "other_party_number string")
FEE_SCHEMA = "tenant_id string, id string, agreement_id string, charge_type string, amount double, charged_at timestamp"


def _ts(s):
    from datetime import datetime, timezone
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


@pytest.fixture()
def silver(spark):
    dim = spark.createDataFrame([
        ("tenant_a", "s1", "g1", 400, 2, False, False, True),
        ("tenant_a", "s2", "g2", 10, 0, True, True, True),
        ("tenant_b", "s3", "g3", 100, 1, False, False, True),
        ("tenant_a", "s4", "g4", 50, 1, False, False, False),   # no analytics consent
    ], DIM_SCHEMA)
    rech = spark.createDataFrame([
        ("tenant_a", "r1", "s1", _ts("2026-03-31T00:00:00"), 10.0, "successful", "credit_card"),
        ("tenant_a", "r2", "s1", _ts("2026-01-15T00:00:00"), 30.0, "successful", "cash"),
        ("tenant_a", "r3", "s1", _ts("2026-03-30T00:00:00"), 50.0, "failed", "cash"),
        ("tenant_b", "r4", "s3", _ts("2026-03-01T00:00:00"), 20.0, "successful", "voucher"),
        ("tenant_a", "r5", "s4", _ts("2026-02-01T00:00:00"), 500.0, "successful", "cash"),
    ], RECH_SCHEMA)
    usage = spark.createDataFrame([
        ("tenant_a", "u1", "s1", "voice", _ts("2026-03-31T10:00:00"), 2.0, 120.0, False, "originating", "tok_x"),
        ("tenant_a", "u2", "s1", "voice", _ts("2026-03-31T11:00:00"), 1.0, 60.0, True, "terminating", "tok_y"),
        ("tenant_a", "u3", "s1", "data", _ts("2026-01-01T00:00:00"), 4.0, 1000.0, False, None, None),
    ], USAGE_SCHEMA)
    fees = spark.createDataFrame([
        ("tenant_a", "f1", "g1", "recurring", 9.99, _ts("2026-03-01T00:00:00")),
        ("tenant_a", "f2", "g1", "overage", 3.0, _ts("2026-01-01T00:00:00")),
        ("tenant_a", "f3", "unknown", "recurring", 5.0, _ts("2026-03-01T00:00:00")),  # no subscriber
    ], FEE_SCHEMA)
    return {"dim_subscriber": dim, "fact_recharge": rech, "fact_usage": usage, "fact_fee_charge": fees}


def _rows(df):
    return {r["service_id"]: r.asDict() for r in df.collect()}


def test_recharge_windows_and_reliability(silver):
    rows = _rows(features.recharge_aggregates(silver["fact_recharge"], (30, 90)))
    s1 = rows["s1"]
    assert (s1["recharge_count"], s1["failed_recharge_count"]) == (3, 1)
    assert s1["recharge_success_rate"] == pytest.approx(2 / 3)
    assert s1["recharge_total"] == 40.0 and s1["recharge_max"] == 30.0   # failed r3 excluded
    assert s1["recharge_total_30d"] == 10.0                              # ref = 2026-03-31 (whole table)
    assert s1["recharge_total_90d"] == 40.0
    assert s1["days_since_last_recharge"] == 0
    assert rows["s3"]["days_since_last_recharge"] == 30                  # vs the table-wide reference
    assert s1["share_credit_card"] == pytest.approx(1 / 3)


def test_usage_velocity_and_windows(silver):
    s1 = _rows(features.usage_aggregates(silver["fact_usage"], (30,)))["s1"]
    assert s1["voice_minutes"] == 3.0 and s1["voice_call_count"] == 2
    assert s1["originating_ratio"] == 0.5 and s1["roaming_share"] == pytest.approx(1 / 3)
    assert s1["unique_called_numbers"] == 2 and s1["calls_per_active_day"] == 2.0
    assert s1["usage_charged_amount"] == 7.0 and s1["usage_charged_amount_30d"] == 3.0
    assert s1["data_bytes"] == 1000.0 and s1["data_bytes_30d"] == 0.0


def test_fee_attribution_via_agreement(silver):
    rows = _rows(features.fee_aggregates(silver["fact_fee_charge"], silver["dim_subscriber"], (30,)))
    assert set(rows) == {"s1"}                                           # f3 has no subscriber
    s1 = rows["s1"]
    assert s1["fee_total"] == pytest.approx(12.99) and s1["fee_charge_count"] == 2
    assert s1["fee_recurring_total"] == pytest.approx(9.99) and s1["fee_overage_count"] == 1
    assert s1["fee_total_30d"] == pytest.approx(9.99)


def test_credit_features_contract_and_derivations(silver):
    df = features.build_credit_features(silver, (30, 90))
    assert df.columns == ["tenant_id", "service_id", *CREDIT_FEATURES]
    rows = _rows(df)
    assert set(rows) == {"s1", "s2", "s3"}                               # one row per dim subscriber
    assert rows["s2"]["recharge_count"] == 0 and rows["s2"]["fee_total"] == 0.0   # no activity -> 0
    assert rows["s2"]["has_device_agreement"] == 1 and rows["s2"]["is_company"] == 1
    assert rows["s1"]["arpu_monthly"] == pytest.approx(7.0 / (400 / 30), abs=1e-4)
    assert rows["s1"]["spend_to_recharge_ratio"] == pytest.approx(7.0 / 40.0, abs=1e-4)
    assert rows["s2"]["spend_to_recharge_ratio"] == 0.0


def test_fraud_zscore_is_per_tenant(silver):
    df = features.build_fraud_features(silver, (30, 90))
    assert df.columns == ["tenant_id", "service_id", *FRAUD_FEATURES]
    rows = _rows(df)
    # tenant_a recharge_max: s1=30, s2=0 -> z = +1 / -1 ; tenant_b has one subscriber -> 0
    assert rows["s1"]["recharge_amount_zscore"] == pytest.approx(1.0)
    assert rows["s2"]["recharge_amount_zscore"] == pytest.approx(-1.0)
    assert rows["s3"]["recharge_amount_zscore"] == 0.0


# --- REQ-FND-05: analytics consent (fail closed) ------------------------------
def test_consenting_subscribers_fails_closed(spark):
    from idp.common.pii import ConsentError
    dim = spark.createDataFrame(
        [("tenant_a", "yes", True), ("tenant_a", "no", False), ("tenant_a", "unknown", None)],
        "tenant_id string, service_id string, consent_analytics boolean")
    kept = {r["service_id"] for r in features.consenting_subscribers(dim).collect()}
    assert kept == {"yes"}                                   # SCN-FND-05.3: null == no consent
    with pytest.raises(ConsentError, match="consent_analytics"):
        features.consenting_subscribers(dim.drop("consent_analytics"))


def test_non_consenting_subscriber_excluded_from_both_tables(silver):
    # SCN-FND-05.1 — s4 has activity but no consent: no feature row, and its large
    # recharge does not leak into the per-tenant z-score of the others
    credit = _rows(features.build_credit_features(silver, (30, 90)))
    fraud = _rows(features.build_fraud_features(silver, (30, 90)))
    assert set(credit) == set(fraud) == {"s1", "s2", "s3"}   # every consenting subscriber once
    assert fraud["s1"]["recharge_amount_zscore"] == pytest.approx(1.0)


def test_gold_builds_each_shared_aggregate_once(lake, monkeypatch):
    # the credit and fraud tables share the recharge and usage aggregates: one Gold
    # run builds each once (and fees once), instead of once per table
    from idp.pipeline import to_gold
    from idp.transforms import features
    calls = {"recharge": 0, "usage": 0, "fees": 0}
    for name, fn in (("recharge", features.recharge_aggregates), ("usage", features.usage_aggregates),
                     ("fees", features.fee_aggregates)):
        def spy(*a, _fn=fn, _name=name, **k):
            calls[_name] += 1
            return _fn(*a, **k)
        monkeypatch.setattr(features, f"{name if name != 'fees' else 'fee'}_aggregates", spy)
    to_gold(lake)
    assert calls == {"recharge": 1, "usage": 1, "fees": 1}
