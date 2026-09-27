"""The lake's Iceberg catalog from Python (REQ-FND-04)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo

from conftest import lake_cfg
from idp.common import catalog, storage


@pytest.fixture()
def cfg(tmp_path):
    return lake_cfg(tmp_path)


def _df(rows):
    return pd.DataFrame(rows, columns=["tenant_id", "id", "amount"])


def test_create_places_table_in_its_layer_and_partitions_by_tenant(cfg):
    # SCN-FND-04.2 — local: data under the configured layer path
    catalog.append_table(cfg, "bronze", "recharge_events", _df([("tenant_a", "1", 1.0)]))
    t = catalog.load_catalog(cfg).load_table("bronze.recharge_events")
    assert t.location() == catalog.table_location(cfg, "bronze", "recharge_events")
    assert Path(storage.lake_uri(cfg, "bronze")).resolve().as_uri() in t.location()
    assert [f.name for f in t.spec().fields] == ["tenant_id"]


def test_table_location_uses_layer_bucket_with_s3(monkeypatch, cfg):
    # SCN-FND-04.1 — S3: data under s3://<prefix>-<layer>, apart from legacy Parquet
    monkeypatch.setenv("IDP_S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("IDP_S3_BUCKET_PREFIX", "idp")
    assert catalog.table_location(cfg, "silver", "fact_usage") == "s3://idp-silver/tables/fact_usage"


def test_append_is_one_snapshot_and_evolves_schema(cfg):
    catalog.append_table(cfg, "bronze", "t", _df([("tenant_a", "1", 1.0)]))
    df2 = _df([("tenant_b", "2", 2.0)]).assign(currency="EUR")      # a new column
    catalog.append_table(cfg, "bronze", "t", df2)
    t = catalog.load_catalog(cfg).load_table("bronze.t")
    assert len(t.snapshots()) == 2
    got = catalog.read_table(cfg, "bronze", "t").sort_values("id")
    assert got["currency"].isna().tolist() == [True, False] and got["currency"].iloc[1] == "EUR"


def test_absent_and_all_null_columns(cfg):
    catalog.append_table(cfg, "bronze", "t", _df([("tenant_a", "1", 1.0)]).assign(code="x"))
    # a later batch lacks `code` entirely and carries an all-null `reason`
    batch = _df([("tenant_a", "2", 2.0)]).assign(reason=[None])
    catalog.append_table(cfg, "bronze", "t", batch)
    got = catalog.read_table(cfg, "bronze", "t").sort_values("id")
    assert got["code"].isna().tolist() == [False, True]
    assert got["reason"].isna().all()


def test_filtered_read_prunes_other_tenants(cfg):
    # SCN-FND-04.4
    catalog.append_table(cfg, "gold", "t", _df([("tenant_a", "1", 1.0), ("tenant_b", "2", 2.0)]))
    t = catalog.load_catalog(cfg).load_table("gold.t")
    files = list(t.scan(row_filter=EqualTo("tenant_id", "tenant_a")).plan_files())
    assert files and all("tenant_id=tenant_a" in f.file.file_path for f in files)
    got = catalog.read_table(cfg, "gold", "t", row_filter=EqualTo("tenant_id", "tenant_a"))
    assert got["tenant_id"].tolist() == ["tenant_a"]


def test_overwrite_tenants_replaces_only_those_tenants(cfg):
    catalog.append_table(cfg, "gold", "t", _df([("tenant_a", "1", 1.0), ("tenant_b", "2", 2.0)]))
    catalog.overwrite_tenants(cfg, "gold", "t", _df([("tenant_a", "9", 9.0)]))
    got = catalog.read_table(cfg, "gold", "t").sort_values("tenant_id")
    assert got[["tenant_id", "id"]].values.tolist() == [["tenant_a", "9"], ["tenant_b", "2"]]


def test_missing_table_raises(cfg):
    with pytest.raises(NoSuchTableError):
        catalog.read_table(cfg, "silver", "nope")
    assert catalog.table_exists(cfg, "silver", "nope") is False


def test_image_bakes_the_iceberg_jars_the_session_expects():
    # task 6.2 — the Dockerfile's jars and ICEBERG_PACKAGES must not drift apart
    from pathlib import Path
    from idp.transforms.session import ICEBERG_PACKAGES
    dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text()
    for coord in ICEBERG_PACKAGES:
        group, artifact, version = coord.split(":")
        assert f"{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.jar" in dockerfile, coord


def test_layer_namespaces_carry_their_location(tmp_path):
    # Nessie places tables at <namespace location>/<table>_<uuid>, so each layer
    # namespace must point into its own bucket/dir, under tables/
    from conftest import lake_cfg
    from idp.common import catalog
    cfg = lake_cfg(tmp_path)
    cat = catalog.load_catalog(cfg)
    for layer in catalog.LAYERS:
        loc = cat.load_namespace_properties(layer)["location"]
        assert loc == (tmp_path / layer / "tables").resolve().as_uri()
    cfg.raw["s3"] = {"enabled": True, "bucket_prefix": "idp"}
    assert catalog.namespace_location(cfg, "gold") == "s3://idp-gold/tables"
