"""File landing zone (REQ-BRZ-03): a simulated source file drop, written by
``idp generate``.

Each generated entity is written as a JSON array of records to
``<landing>/tenant=<tenant_id>/<entity>.json``, one file per tenant and entity.
The layout mirrors a per-tenant source pickup (in production, e.g. NiFi). It
doesn't write Bronze: Bronze is the Iceberg tables ``bronze.<entity>``, written by
``pipeline.to_bronze`` (from ``idp consume``, ``idp run`` and ``idp migrate-lake``).
"""
from __future__ import annotations

import json
from pathlib import Path

from idp.common.tenancy import TENANT_COLUMN, require_tenant_column
from idp.synthetic.generator import ENTITIES, SyntheticDataset


def write_landing(dataset: SyntheticDataset, landing_dir: str | Path) -> dict[str, list[Path]]:
    """Write source records as JSON files to the landing zone (simulates NiFi pickup).

    Files are written per ``tenant=<id>/{entity}.json`` so the layout mirrors the
    per-tenant landing structure in plan 3.2.
    """
    landing = Path(landing_dir)
    written: dict[str, list[Path]] = {}
    for entity in ENTITIES:
        df = getattr(dataset, entity)
        if df.empty:
            continue
        require_tenant_column(df)
        paths = []
        for tenant_id, part in df.groupby(TENANT_COLUMN):
            out_dir = landing / f"tenant={tenant_id}"
            out_dir.mkdir(parents=True, exist_ok=True)
            path = out_dir / f"{entity}.json"
            records = part.to_dict(orient="records")
            path.write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
            paths.append(path)
        written[entity] = paths
    return written
