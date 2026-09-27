"""Tests for the file landing zone (REQ-BRZ-03) and the shared entity list.

Bronze itself (Iceberg, `pipeline.to_bronze`) is covered by test_bronze_iceberg.py.
"""
from __future__ import annotations

from idp.ingestion import landing
from idp.synthetic.generator import ENTITIES


def test_write_landing_partitions_by_tenant(multi_tenant_dataset, tmp_path):
    written = landing.write_landing(multi_tenant_dataset, tmp_path)
    assert "parties" in written
    assert (tmp_path / "tenant=tenant_a" / "parties.json").exists()
    assert (tmp_path / "tenant=tenant_b" / "parties.json").exists()


def test_landing_has_one_file_per_tenant_entity(multi_tenant_dataset, tmp_path):
    import json

    landing.write_landing(multi_tenant_dataset, tmp_path)
    for tenant_id in ("tenant_a", "tenant_b"):
        for entity in ENTITIES:
            if getattr(multi_tenant_dataset, entity).empty:
                continue
            path = tmp_path / f"tenant={tenant_id}" / f"{entity}.json"
            records = json.loads(path.read_text(encoding="utf-8"))
            assert records and {r["tenant_id"] for r in records} == {tenant_id}


def test_generate_command_writes_landing_not_bronze(tmp_path):
    import yaml

    from idp.cli import main
    from idp.common.config import DEFAULT_CONFIG_PATH

    raw = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["storage"] = {k: str(tmp_path / k) for k in
                      ("root", "landing", "bronze", "silver", "gold", "ml_artifacts")}
    raw["synthetic"].update({"subscribers_per_tenant": 10, "days": 5})
    cfg_path = tmp_path / "platform.yaml"
    cfg_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    assert main(["--config", str(cfg_path), "generate"]) == 0
    assert (tmp_path / "landing" / "tenant=tenant_a" / "parties.json").exists()
    assert not (tmp_path / "bronze").exists()


# --- one entity list ----------------------------------------------------------
def test_entities_defined_once_and_cover_dataset_and_topics():
    from dataclasses import fields

    from idp import pipeline
    from idp.streaming import TABLE_TO_SUFFIX
    from idp.synthetic import generator
    from idp.synthetic.generator import ENTITIES, SyntheticDataset

    assert pipeline.ENTITIES is generator.ENTITIES                 # a re-export, not a copy
    assert ENTITIES == tuple(f.name for f in fields(SyntheticDataset) if f.name != "labels")
    assert set(TABLE_TO_SUFFIX) == set(ENTITIES)
