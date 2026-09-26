"""TimescaleDB serving layer (plan Q4/Q6).

Mirrors Gold tables into TimescaleDB/PostgreSQL for low-latency serving (APIs,
dashboards). Gold on S3 is the analytical store; TimescaleDB is the fast,
indexed serving copy queried by ``service_id``.

Kept dependency-light: SQLAlchemy + psycopg2. Imported lazily by callers so the
rest of the platform needs neither unless serving is used.
"""
from __future__ import annotations

import pandas as pd

from idp.common.config import PlatformConfig

KEY_COLUMNS = ("tenant_id", "service_id")


def make_url(cfg: PlatformConfig) -> str:
    t = cfg.timescale
    return (
        f"postgresql+psycopg2://{t['user']}:{t['password']}"
        f"@{t['host']}:{t['port']}/{t['database']}"
    )


def get_engine(cfg: PlatformConfig):
    from sqlalchemy import create_engine
    return create_engine(make_url(cfg), pool_pre_ping=True)


def publish_frame(engine, name: str, df: pd.DataFrame, schema: str | None = None) -> None:
    """Replace a serving table with ``df``, uniquely indexed by (tenant_id, service_id).

    The unique index enforces one serving row per subscriber (REQ-SRV-05). A frame
    with duplicate keys fails the index build inside the transaction, so the table
    previously published stays intact.

    Concurrency-safe: ``if_exists="replace"`` does DROP+CREATE, and two publishers
    running at once (e.g. ``make publish`` overlapping the inference DAG's publish
    task) would race on the Postgres type catalog — `duplicate key value violates
    unique constraint "pg_type_typname_nsp_index"`. We serialize per-table writers
    with a transaction-scoped **advisory lock** and do the whole replace in one
    transaction, so concurrent publishers wait their turn instead of colliding.
    """
    from sqlalchemy import text

    # The advisory lock below is Postgres-only; skip it on other backends (e.g. the
    # SQLite engine used in tests), which lack pg_advisory_xact_lock/hashtext and
    # have no cross-process DDL race to guard against.
    is_postgres = engine.dialect.name == "postgresql"
    qualified = f'"{schema}"."{name}"' if schema else f'"{name}"'
    idx_cols = [c for c in KEY_COLUMNS if c in df.columns]
    with engine.begin() as conn:
        if schema:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        # Serialize concurrent publishers of THIS table (released at commit).
        if is_postgres:
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"),
                         {"k": f"{schema or ''}.{name}"})
        df.to_sql(name, conn, schema=schema, if_exists="replace", index=False,
                  method="multi", chunksize=1000)
        if idx_cols:
            cols = ", ".join(f'"{c}"' for c in idx_cols)
            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_{name}_key" ON {qualified} ({cols})'))


def publish_gold(cfg: PlatformConfig, gold: dict[str, pd.DataFrame], engine=None) -> list[str]:
    """Write all Gold tables to the serving DB. Returns the tables published."""
    eng = engine or get_engine(cfg)
    schema = cfg.timescale.get("schema")
    published = []
    for name, df in gold.items():
        if df is None or df.empty:
            continue
        publish_frame(eng, name, df, schema)
        published.append(name)
    return published


def read_score(cfg, table: str, tenant_id: str, service_id: str, engine=None) -> dict | None:
    """Fetch one precomputed serving row by (tenant_id, service_id)."""
    from sqlalchemy import text

    eng = engine or get_engine(cfg)
    schema = cfg.timescale.get("schema")
    qualified = f'"{schema}"."{table}"' if schema else f'"{table}"'
    sql = text(f"SELECT * FROM {qualified} WHERE tenant_id = :t AND service_id = :s LIMIT 1")
    with eng.connect() as conn:
        row = conn.execute(sql, {"t": tenant_id, "s": service_id}).mappings().first()
    return dict(row) if row else None
