"""The committed Superset content bundle (REQ-BI-05): tenant-free, credential-free."""
from __future__ import annotations

import json
from pathlib import Path

import yaml

from idp.common.config import load_config

CONTENT = Path(__file__).resolve().parents[1] / "deploy" / "platform" / "superset" / "content"


def _files():
    return {p.relative_to(CONTENT).as_posix(): p.read_text() for p in sorted(CONTENT.rglob("*.yaml"))}


def _docs(prefix):
    return {k: yaml.safe_load(v) for k, v in _files().items() if k.startswith(prefix)}


def test_bundle_holds_the_starter_content():
    # SCN-BI-05.1 (shape): two databases, eight datasets (two virtual), 20 charts, four dashboards
    files = _files()
    count = lambda d: sum(1 for k in files if k.startswith(d))          # noqa: E731
    assert (count("databases/"), count("datasets/"), count("charts/"), count("dashboards/")) == (2, 8, 20, 4)
    tables = {d["table_name"] for d in _docs("datasets/").values()}
    assert tables == {"dim_subscriber", "fact_usage", "fact_recharge", "fact_fee_charge",
                      "credit_scores", "fraud_scores", "usage_windows", "overage_fee_windows"}
    assert {d["dashboard_title"] for d in _docs("dashboards/").values()} == \
           {"Subscriber overview", "Usage overview", "Credit scores", "Fraud scores"}


def test_no_tenant_in_the_content():
    # SCN-BI-05.2: the same dashboards serve every tenant
    tenants = [t.id for t in load_config().tenants]
    for name, text in _files().items():
        for t in tenants:
            assert t not in text, f"{name} names tenant {t}"


def test_no_credential_in_the_content():
    # SCN-BI-05.2 / SCN-PLT-12.2: connections carry service user names only; the
    # connection mutator supplies every credential per signed-in user
    for name, db in _docs("databases/").items():
        uri = db["sqlalchemy_uri"]
        user_part = uri.split("://", 1)[1].split("@", 1)[0]
        assert ":" not in user_part, f"{name}: password in {uri}"
        assert db["allow_dml"] is False and db["allow_file_upload"] is False
        assert not db.get("allow_ctas") and not db.get("allow_cvas")
    for name, text in _files().items():
        lowered = text.lower()
        assert "password:" not in lowered and "secret" not in lowered, name


def test_every_chart_has_a_stored_query():
    # the chart-data API (and make superset-smoke) needs each chart's query context
    for name, chart in _docs("charts/").items():
        qc = json.loads(chart["query_context"])
        assert qc["queries"] and qc["datasource"]["type"] == "table", name


def test_dashboard_layouts_place_every_chart_by_uuid():
    # a layout entry without the chart's uuid points at a chart id from the exporting
    # instance; on import it renders as an empty box and the chart lands below it
    uuids = {c["uuid"] for c in _docs("charts/").values()}
    placed = []
    for name, dash in _docs("dashboards/").items():
        pos = dash["position"]
        for key, item in pos.items():
            if isinstance(item, dict) and item.get("type") == "CHART":
                assert item["meta"].get("uuid") in uuids, f"{name}: {key} is not linked to a bundled chart"
                placed.append(item["meta"]["uuid"])
            for child in (item.get("children", []) if isinstance(item, dict) else []):
                assert child in pos, f"{name}: {key} lists missing {child}"
    assert sorted(placed) == sorted(uuids)            # each chart once, none left out
