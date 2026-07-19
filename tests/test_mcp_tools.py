"""Phase 11 — MCP analytics tools, synthetic fee charges, and Silver fee fact.

Covers REQ-SYN-04, REQ-SLV-05, and REQ-MCP-01…06. The five tools read a locally
persisted Silver lake (s3 disabled); a per-test config points storage at a tmp
dir and `load_config` in the tools module is monkeypatched to return it.
"""
from __future__ import annotations

import pytest

from idp.common.config import PlatformConfig, StorageConfig, Tenant
from idp.curation.silver import build_fact_fee_charge, curate
from idp.mcp import tools as mcp_tools
from idp.pipeline import ENTITIES, to_bronze, to_silver
from idp.synthetic.generator import generate, generate_for_tenants

SALTS = {"tenant_a": "salt-a", "tenant_b": "salt-b"}


# --------------------------------------------------------------------------- #
# REQ-SYN-04 — synthetic fee charges + catalog/recharge enrichment            #
# --------------------------------------------------------------------------- #
def test_fee_charges_are_normalized_and_typed():
    ds = generate("tenant_a", subscribers=60, days=40, seed=7)
    fees = ds.fee_charges
    assert not fees.empty
    # Normalized: references a product only; no denormalized name/category/service.
    assert {"tenant_id", "id", "product_id", "amount", "charge_type", "charged_at"} <= set(fees.columns)
    assert not ({"service_id", "agreement_id", "product_name", "category"} & set(fees.columns))
    # referential: every product_id resolves to a generated product
    assert set(fees["product_id"]) <= set(ds.products["id"])
    assert set(fees["charge_type"]) <= {"recurring", "one_time", "overage", "discount"}
    assert (fees.loc[fees["charge_type"] == "discount", "amount"] < 0).all()


def test_products_enriched_and_recharge_types_varied():
    ds = generate("tenant_a", subscribers=80, days=40, seed=7)
    assert {"category", "monthly_fee"} <= set(ds.products.columns)
    assert ds.products["monthly_fee"].gt(0).all()
    assert ds.recharge_events["recharge_type"].nunique() > 1


def test_generation_is_deterministic_for_fees():
    a = generate("tenant_a", subscribers=40, days=40, seed=7).fee_charges
    b = generate("tenant_a", subscribers=40, days=40, seed=7).fee_charges
    assert a.equals(b)


# --------------------------------------------------------------------------- #
# REQ-SLV-05 — fact_fee_charge                                                 #
# --------------------------------------------------------------------------- #
def test_fact_fee_charge_enriched_from_products(bronze_frames):
    silver = curate(bronze_frames, SALTS)
    assert "fact_fee_charge" in silver
    fee = silver["fact_fee_charge"]
    assert not fee.empty
    # product_name/category re-derived by joining products; charge carries no PII
    assert {"product_id", "product_name", "category"} <= set(fee.columns)
    assert "service_id" not in fee.columns
    assert fee["product_name"].notna().all()
    assert fee["category"].notna().all()


def test_build_fact_fee_charge_empty_is_safe():
    assert build_fact_fee_charge({}, SALTS).empty


# --------------------------------------------------------------------------- #
# Local Silver lake fixture for the MCP tools                                  #
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def local_cfg(tmp_path_factory):
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
    frames = {e: getattr(ds, e) for e in ENTITIES}
    to_bronze(cfg, frames, persist=True)
    to_silver(cfg, frames, persist=True)
    return cfg


@pytest.fixture()
def patched(local_cfg, monkeypatch):
    monkeypatch.setattr(mcp_tools, "load_config", lambda _p=None: local_cfg)
    return local_cfg


# --------------------------------------------------------------------------- #
# REQ-MCP-01…05 — the five tools                                              #
# --------------------------------------------------------------------------- #
def test_subscribers_tool(patched):
    out = mcp_tools.analyze_subscribers_tool("tenant_a")
    assert out["subscription_count"] + out["device_count"] == out["subscriber_count"]
    assert sum(out["business_line_mix"].values()) == out["subscriber_count"]
    assert out["roamers"] + out["non_roamers"] == out["subscriber_count"]


def test_usage_events_tool(patched):
    out = mcp_tools.analyze_usage_events_tool("tenant_a", granularity="month")
    assert out["total_events"] == sum(out["counts_by_event_type"].values())
    assert set(out["revenue_by_event_type"]) <= {"voice", "data"}
    assert out["usage_over_time"]


def test_recharges_tool(patched):
    out = mcp_tools.analyze_recharges_tool("tenant_a", granularity="month")
    assert out["total_topup"] > 0
    assert out["recharge_count"] >= out["successful_count"] > 0
    approx = out["total_topup"] / out["successful_count"]
    assert abs(out["average_topup"] - approx) < 0.05
    assert out["by_recharge_type"]["count"]


def test_recharge_anomalies_tool(patched):
    out = mcp_tools.analyze_recharge_anomalies_tool("tenant_a", sensitivity=3.0, view="monthly")
    assert "amount_spikes" in out and "volume_spikes" in out
    assert isinstance(out["subscriber_day_anomalies"], list)
    assert out["periodic_view"]


def test_fee_charges_tool(patched):
    out = mcp_tools.analyze_fee_charges_tool("tenant_a", breakdown="quarter")
    assert out["total_fee_revenue"] > 0
    assert out["revenue_by_product"]
    # shares sum to ~1.0
    assert abs(sum(out["revenue_share_by_product"].values()) - 1.0) < 0.01
    assert set(out["by_charge_type"]["revenue"]) <= {"recurring", "one_time", "overage", "discount"}


# --------------------------------------------------------------------------- #
# REQ-MCP-06 — tenant isolation, no raw PII, missing tenant handled           #
# --------------------------------------------------------------------------- #
def test_tenant_isolation_no_raw_pii(patched):
    out = mcp_tools.analyze_recharge_anomalies_tool("tenant_a", sensitivity=0.0)
    ids = [r["service_id"] for r in out["amount_spikes"]] + [r["service_id"] for r in out["volume_spikes"]]
    assert all(str(i).startswith("tok_") for i in ids)
    assert not any(str(i).startswith("5959") for i in ids)


def test_missing_tenant_returns_structured_not_raise(patched):
    for fn in mcp_tools.TOOLS.values():
        out = fn("tenant_does_not_exist")
        assert isinstance(out, dict) and out["tenant_id"] == "tenant_does_not_exist"


def test_registry_has_five_tools():
    assert len(mcp_tools.TOOLS) == 5


def test_server_builds_and_registers_tools():
    pytest.importorskip("mcp")
    from idp.mcp.server import build_server

    server = build_server()
    assert server is not None
