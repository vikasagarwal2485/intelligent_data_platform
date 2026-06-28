"""Bronze (raw, immutable) ingestion (plan section 3.2).

In production: NiFi stamps ``tenant_id`` and publishes Avro to Kafka; a Kafka
Connect S3 sink lands the raw records to ``bronze/tenant=<id>/{entity}/...``.

In the prototype: we persist the raw frames to Parquet partitioned by
``tenant_id`` (the leading partition column that guarantees physical data
isolation, plan 1.4/1.5). The data is treated as immutable.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from idp.common.tenancy import TENANT_COLUMN, require_tenant_column
from idp.synthetic.generator import SyntheticDataset

ENTITIES = (
    "parties", "accounts", "agreements", "products",
    "voice_events", "data_events", "recharge_events",
)


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


def persist_bronze(frames: dict[str, pd.DataFrame], bronze_dir: str | Path) -> None:
    """Persist raw frames to Parquet partitioned by tenant_id (immutable Bronze)."""
    bronze = Path(bronze_dir)
    for entity, df in frames.items():
        if df is None or df.empty:
            continue
        require_tenant_column(df)
        out_dir = bronze / entity
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_dir, partition_cols=[TENANT_COLUMN], index=False)


def load_bronze(bronze_dir: str | Path) -> dict[str, pd.DataFrame]:
    """Load all Bronze entity frames back from Parquet."""
    bronze = Path(bronze_dir)
    frames: dict[str, pd.DataFrame] = {}
    for entity in ENTITIES:
        path = bronze / entity
        if path.exists():
            df = pd.read_parquet(path)
            # partition column comes back as categorical; normalize to str
            if TENANT_COLUMN in df.columns:
                df[TENANT_COLUMN] = df[TENANT_COLUMN].astype(str)
            frames[entity] = df
    return frames


def ingest(dataset: SyntheticDataset, bronze_dir: str | Path) -> dict[str, pd.DataFrame]:
    """Ingest a generated dataset into Bronze and return the persisted frames."""
    frames = {entity: getattr(dataset, entity) for entity in ENTITIES}
    persist_bronze(frames, bronze_dir)
    return frames
