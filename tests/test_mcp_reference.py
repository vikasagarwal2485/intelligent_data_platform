"""The pandas reference tools (``tests/mcp_reference.py``) on a local lake.

These are the MCP tool tests from before ``mcp-over-trino``, retargeted to the
frozen oracle: they show that it still reproduces the original expectations
(REQ-MCP-01…08, REQ-FND-06), so it can stand as the parity reference for the
Trino-backed tools.
"""
from __future__ import annotations

import pandas as pd
import pytest

import mcp_reference as ref
from idp.ingestion.raw import raw_events
from idp.common.config import PlatformConfig, StorageConfig, Tenant
from idp.pipeline import ENTITIES, to_bronze, to_silver
from idp.synthetic.generator import generate_for_tenants

SALTS = {"tenant_a": "salt-a", "tenant_b": "salt-b"}


# --------------------------------------------------------------------------- #
# Local Silver lake fixture for the MCP tools                                  #
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def local_cfg(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("lake")
    storage = StorageConfig(
        root=str(tmp), landing=str(tmp / "landing"), bronze=str(tmp / "bronze"),
        silver=str(tmp / "silver"), gold=str(tmp / "gold"), ml_artifacts=str(tmp / "ml"),
    )
    tenants = [
        Tenant(id="tenant_a", name="A", region="eu", pii_salt=SALTS["tenant_a"]),
        Tenant(id="tenant_b", name="B", region="eu", pii_salt=SALTS["tenant_b"]),
    ]
    raw = {"s3": {"enabled": False}, "features": {"windows": [30, 90]}}
    cfg = PlatformConfig(raw=raw, tenants=tenants, storage=storage)

    ds = generate_for_tenants(["tenant_a", "tenant_b"], subscribers=60, days=40, fraud_rate=0.2, seed=7)
    frames = raw_events(ds)
    to_bronze(cfg, frames, persist=True)
    to_silver(cfg)
    return cfg


@pytest.fixture()
def patched(local_cfg, monkeypatch):
    monkeypatch.setattr(ref, "load_config", lambda _p=None: local_cfg)
    return local_cfg


# --------------------------------------------------------------------------- #
# REQ-MCP-01…05 — the five tools                                              #
# --------------------------------------------------------------------------- #
def test_subscribers_tool(patched):
    out = ref.analyze_subscribers_tool("tenant_a")
    assert out["subscription_count"] + out["device_count"] == out["subscriber_count"]
    assert sum(out["business_line_mix"].values()) == out["subscriber_count"]
    assert out["roamers"] + out["non_roamers"] == out["subscriber_count"]


def test_usage_events_tool(patched):
    out = ref.analyze_usage_events_tool("tenant_a", granularity="month")
    assert out["total_events"] == sum(out["counts_by_event_type"].values())
    assert set(out["revenue_by_event_type"]) <= {"voice", "data"}
    assert out["usage_over_time"]


def test_recharges_tool(patched):
    out = ref.analyze_recharges_tool("tenant_a", granularity="month")
    assert out["total_topup"] > 0
    assert out["recharge_count"] >= out["successful_count"] > 0
    approx = out["total_topup"] / out["successful_count"]
    assert abs(out["average_topup"] - approx) < 0.05
    assert out["by_recharge_type"]["count"]


def test_recharge_anomalies_tool(patched):
    out = ref.analyze_recharge_anomalies_tool("tenant_a", sensitivity=3.0, view="monthly")
    assert "amount_spikes" in out and "volume_spikes" in out
    assert isinstance(out["subscriber_day_anomalies"], list)
    assert out["periodic_view"]


def test_fee_charges_tool(patched):
    out = ref.analyze_fee_charges_tool("tenant_a", breakdown="quarter")
    assert out["total_fee_revenue"] > 0
    assert out["revenue_by_product"]
    # shares sum to ~1.0
    assert abs(sum(out["revenue_share_by_product"].values()) - 1.0) < 0.01
    assert set(out["by_charge_type"]["revenue"]) <= {"recurring", "one_time", "overage", "discount"}


# --------------------------------------------------------------------------- #
# REQ-MCP-06 — tenant isolation, no raw PII, missing tenant handled           #
# --------------------------------------------------------------------------- #
def test_tenant_isolation_no_raw_pii(patched):
    out = ref.analyze_recharge_anomalies_tool("tenant_a", sensitivity=0.0)
    ids = [r["service_id"] for r in out["amount_spikes"]] + [r["service_id"] for r in out["volume_spikes"]]
    assert all(str(i).startswith("tok_") for i in ids)
    assert not any(str(i).startswith("5959") for i in ids)


def test_missing_tenant_returns_structured_not_raise(patched):
    for fn in ref.TOOLS.values():
        out = fn("tenant_does_not_exist")
        assert isinstance(out, dict) and out["tenant_id"] == "tenant_does_not_exist"


def test_unreachable_lake_surfaces_error_not_empty(patched, monkeypatch):
    # A connectivity failure (e.g. MinIO port-forward down) must surface as an
    # explicit error, not a misleading empty "0" result.
    from idp.common import catalog

    def boom(*a, **k):
        raise ConnectionError("Connection refused")
    monkeypatch.setattr(catalog, "load_catalog", boom)
    out = ref.analyze_subscribers_tool("tenant_a")
    assert out["tenant_id"] == "tenant_a"
    assert "Connection refused" in out["error"]
    assert "source" in out
    assert "subscriber_count" not in out


def test_absent_lake_reports_no_tenants(tmp_path, monkeypatch):
    # A genuinely empty lake (nothing written yet) reports no tenants, not a crash.
    from conftest import lake_cfg
    empty = lake_cfg(tmp_path)
    monkeypatch.setattr(ref, "load_config", lambda _p=None: empty)
    out = ref.analyze_subscribers_tool("tenant_a")
    assert out["available_tenants"] == [] and "error" in out


def test_missing_tenant_asks_with_available_list(patched):
    # No/unknown tenant -> return the real tenants + an instruction to ask the user.
    out = ref.analyze_subscribers_tool("")
    assert set(out["available_tenants"]) == {"tenant_a", "tenant_b"}
    assert "action_required" in out
    assert "subscriber_count" not in out
    # an unknown tenant is handled the same way
    out2 = ref.analyze_fee_charges_tool("tenant_zzz")
    assert set(out2["available_tenants"]) == {"tenant_a", "tenant_b"}


# --------------------------------------------------------------------------- #
# REQ-MCP-07/08 — fraud & credit score distributions over 7/15/30-day windows #
# --------------------------------------------------------------------------- #
def _cfg_with_fresh_gold(local_cfg, tmp_path):
    """A per-test lake with a copy of the module's Silver (for the tenant guard) and
    an empty Gold, so score appends never accumulate across tests."""
    from conftest import clone_lake
    return clone_lake(local_cfg, tmp_path / "lake", layers=("silver",), tables={"dim_subscriber"})


@pytest.fixture()
def scored_cfg(local_cfg, tmp_path, monkeypatch):
    """Write Gold score tables (fraud_scores/credit_scores) with a scored_at spread
    over ~40 days so the 7/15/30-day windows are genuinely different sizes."""
    cfg = _cfg_with_fresh_gold(local_cfg, tmp_path)
    now = pd.Timestamp.now(tz="UTC")
    ages = [1, 1, 5, 12, 12, 25, 25, 25, 38, 38]  # days back
    fraud = pd.DataFrame({
        "tenant_id": "tenant_a",
        "service_id": [f"tok_{i}" for i in range(len(ages))],
        "fraud_proba": [0.05, 0.9, 0.2, 0.6, 0.1, 0.3, 0.8, 0.15, 0.5, 0.25],
        "rule_score": [0.0, 1.0, 0.2, 0.4, 0.0, 0.2, 0.6, 0.0, 0.4, 0.2],
        "fraud_score": [0.04, 0.85, 0.22, 0.55, 0.08, 0.28, 0.74, 0.12, 0.48, 0.24],
        "scored_at": [now - pd.Timedelta(days=d) for d in ages],
    })
    credit = pd.DataFrame({
        "tenant_id": "tenant_a",
        "service_id": [f"tok_{i}" for i in range(len(ages))],
        "pd": [0.05, 0.5, 0.2, 0.35, 0.1, 0.25, 0.45, 0.12, 0.3, 0.22],
        "affordability_base": [100.0, 60.0, 80.0, 50.0, 120.0, 90.0, 40.0, 110.0, 70.0, 85.0],
        "recommended_limit": [90.0, 30.0, 60.0, 32.0, 108.0, 67.0, 22.0, 96.0, 49.0, 66.0],
        "scored_at": [now - pd.Timedelta(days=d) for d in ages],
    })
    from idp.common import catalog
    catalog.append_table(cfg, "gold", "fraud_scores", fraud)
    catalog.append_table(cfg, "gold", "credit_scores", credit)
    monkeypatch.setattr(ref, "load_config", lambda _p=None: cfg)
    return cfg


def test_fraud_score_tool_windows(scored_cfg):
    out = ref.analyze_fraud_score_tool("tenant_a")
    assert out["windows_days"] == [7, 15, 30]
    bw = out["by_window"]
    assert set(bw) == {"7d", "15d", "30d"}
    # window sizes are monotone non-decreasing and reflect the scored_at spread
    assert bw["7d"]["count"] == 3 <= bw["15d"]["count"] == 5 <= bw["30d"]["count"] == 8
    dist = bw["30d"]["fraud_proba"]
    assert dist["count"] == 8 and dist["min"] <= dist["median"] <= dist["max"]
    assert "rule_score" in bw["30d"] and "fraud_score" in bw["30d"]


def test_credit_score_tool_windows_and_gap(scored_cfg):
    out = ref.analyze_credit_score_tool("tenant_a")
    bw = out["by_window"]
    assert bw["7d"]["count"] == 3 and bw["30d"]["count"] == 8
    # affordability_gap = affordability_base - recommended_limit (the risk haircut)
    for w in ("7d", "15d", "30d"):
        assert {"pd", "affordability_base", "recommended_limit", "affordability_gap"} <= set(bw[w])
    assert bw["30d"]["affordability_gap"]["mean"] > 0


def test_score_tools_tenant_isolation(scored_cfg):
    # tenant_b has no scores written -> graceful empty note, not tenant_a's data
    out = ref.analyze_fraud_score_tool("tenant_b")
    assert out["tenant_id"] == "tenant_b" and out["count"] == 0 and "note" in out


def test_score_tools_snapshot_note_without_timestamp(local_cfg, tmp_path, monkeypatch):
    # No scored_at column -> each window falls back to the full snapshot with a note.
    cfg = _cfg_with_fresh_gold(local_cfg, tmp_path)
    frame = pd.DataFrame({
        "tenant_id": "tenant_a", "service_id": ["tok_0", "tok_1"],
        "fraud_proba": [0.2, 0.7], "rule_score": [0.1, 0.5], "fraud_score": [0.2, 0.66],
    })
    from idp.common import catalog
    catalog.append_table(cfg, "gold", "fraud_scores", frame)
    monkeypatch.setattr(ref, "load_config", lambda _p=None: cfg)
    out = ref.analyze_fraud_score_tool("tenant_a")
    assert all(out["by_window"][w]["count"] == 2 for w in ("7d", "15d", "30d"))
    assert "note" in out["by_window"]["7d"]


# --------------------------------------------------------------------------- #
# REQ-FND-06 — tenant-scoped reads go through the gateway                      #
# --------------------------------------------------------------------------- #
def _untagged_copy(local_cfg, tmp_path, table):
    """A lake identical to local_cfg's Silver except ``table`` lacks tenant_id."""
    from conftest import clone_lake
    return clone_lake(local_cfg, tmp_path / "untagged", layers=("silver",), drop_tenant=table)


@pytest.mark.parametrize("table,tool", [
    ("fact_usage", "analyze_usage_events_tool"),       # a fact read by one tool
    ("dim_subscriber", "analyze_subscribers_tool"),    # the table tenant discovery reads
])
def test_untagged_table_is_an_isolation_error(local_cfg, tmp_path, monkeypatch, table, tool):
    # SCN-FND-06.1
    cfg = _untagged_copy(local_cfg, tmp_path, table)
    monkeypatch.setattr(ref, "load_config", lambda _p=None: cfg)
    out = getattr(ref, tool)("tenant_a")
    assert out["error"].startswith("tenant isolation violation") and out["table"] == table
    assert not ({"total_events", "subscriber_count", "business_line_mix"} & set(out))   # no rows served


def test_tools_read_through_the_tenant_gateway(patched, monkeypatch):
    # SCN-FND-06.2 — every tool's rows come from scope_to_tenant for the caller's tenant
    calls = []
    real = ref.scope_to_tenant
    monkeypatch.setattr(ref, "scope_to_tenant",
                        lambda df, t: calls.append(t) or real(df, t))
    for name, fn in ref.TOOLS.items():
        if name in ("analyze_fraud_score_tool", "analyze_credit_score_tool"):
            continue                                   # Gold score tables: covered by scored_cfg tests
        calls.clear()
        fn("tenant_a")
        assert calls and set(calls) == {"tenant_a"}, name
