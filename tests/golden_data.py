"""Fixed input for the golden-file tests (tests/test_golden.py, tests/golden/)."""
from __future__ import annotations

from pathlib import Path

from idp.common.config import PlatformConfig, StorageConfig, Tenant
from idp.ingestion.raw import raw_events
from idp.pipeline import to_bronze
from idp.synthetic.generator import generate_for_tenants

GOLDEN_SALTS = {"tenant_a": "salt-a", "tenant_b": "salt-b"}
GOLDEN_SILVER = ("dim_subscriber", "dim_customer", "dim_billing_account", "dim_product",
                 "fact_usage", "fact_recharge", "fact_fee_charge")
GOLDEN_GOLD = ("credit_features", "fraud_features")
GOLDEN_KEYS = {
    "dim_subscriber": ["tenant_id", "service_id"],
    "dim_customer": ["tenant_id", "customer_id"],
    "dim_billing_account": ["tenant_id", "billing_account_id"],
    "dim_product": ["tenant_id", "product_id"],
    "fact_usage": ["tenant_id", "id"],
    "fact_recharge": ["tenant_id", "id"],
    "fact_fee_charge": ["tenant_id", "id"],
    "credit_features": ["tenant_id", "service_id"],
    "fraud_features": ["tenant_id", "service_id"],
}


def golden_cfg(root: Path) -> PlatformConfig:
    sc = StorageConfig(root=str(root), landing=str(root / "landing"), bronze=str(root / "bronze"),
                       silver=str(root / "silver"), gold=str(root / "gold"),
                       ml_artifacts=str(root / "ml"))
    tenants = [Tenant(t, t, "eu", s) for t, s in GOLDEN_SALTS.items()]
    return PlatformConfig(raw={"s3": {"enabled": False}, "features": {"windows": [7, 30, 90]}},
                          tenants=tenants, storage=sc)


def land_golden_bronze(cfg: PlatformConfig) -> None:
    ds = generate_for_tenants(list(GOLDEN_SALTS), subscribers=20, days=10, fraud_rate=0.2,
                              default_rate=0.2, seed=3)
    # raw Bronze, as consume lands it; a fixed eventTime keeps the payloads stable
    to_bronze(cfg, raw_events(ds, event_time="2026-01-01T00:00:00.000Z"))
