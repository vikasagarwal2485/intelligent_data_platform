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


def _qualified(name: str, schema: str | None) -> str:
    return f'"{schema}"."{name}"' if schema else f'"{name}"'


def staging_name(table: str) -> str:
    """A per-load staging table name: concurrent publishers never share one."""
    import uuid
    return f"{table}__staging_{uuid.uuid4().hex[:8]}"


def swap_in(conn, table: str, staging: str, schema: str | None = None,
            key_columns: tuple[str, ...] | list[str] = KEY_COLUMNS) -> None:
    """Replace ``table`` with the loaded ``staging`` table, atomically (REQ-SRV-01/05).

    Runs inside the caller's transaction (``engine.begin()``):
    1. serialize the publishers of THIS table (Postgres advisory lock, released at commit)
    2. drop the live table (its unique index goes with it)
    3. build the unique ``(tenant_id, service_id)`` index on staging, under its final
       name: duplicate keys fail HERE and the transaction rolls back, so the previous
       table is kept (SCN-SRV-05.2)
    4. rename staging to the live name
    5. on Postgres, re-apply the BI readers' access controls to the new table:
       row-level security, the tenant policy and their ``SELECT`` grant
       (``bi_roles.apply_table_controls``), since dropping the old table dropped them

    Readers see the old table until the commit and the new one after it; never a
    partly loaded table (SCN-SRV-01.5), and never the new one without its policy
    (SCN-SRV-01.6).
    """
    from sqlalchemy import text

    if conn.dialect.name == "postgresql":
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"{schema or ''}.{table}"})
    conn.execute(text(f"DROP TABLE IF EXISTS {_qualified(table, schema)}"))
    if key_columns:
        cols = ", ".join(f'"{c}"' for c in key_columns)
        conn.execute(text(f'CREATE UNIQUE INDEX "ux_{table}_key" ON {_qualified(staging, schema)} ({cols})'))
    conn.execute(text(f'ALTER TABLE {_qualified(staging, schema)} RENAME TO "{table}"'))
    if conn.dialect.name == "postgresql":
        from idp.serving import bi_roles
        bi_roles.apply_table_controls(conn, table, schema)


def publish_frame(engine, name: str, df: pd.DataFrame, schema: str | None = None) -> None:
    """Replace a serving table with ``df``, uniquely indexed by (tenant_id, service_id).

    ``df`` is loaded into a fresh staging table and swapped in (``swap_in``), all in
    one transaction: concurrent publishers queue on the advisory lock, a frame with
    duplicate keys fails and leaves the previous table intact (REQ-SRV-05), and
    readers never see a partial table. (The distributed publish loads the staging
    table with Spark JDBC instead, then calls the same ``swap_in``.)
    """
    from sqlalchemy import text

    staging = staging_name(name)
    key_cols = tuple(c for c in KEY_COLUMNS if c in df.columns)
    with engine.begin() as conn:
        if schema:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        df.to_sql(staging, conn, schema=schema, if_exists="replace", index=False,
                  method="multi", chunksize=1000)
        swap_in(conn, name, staging, schema, key_cols)


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


def jdbc_target(engine) -> tuple[str, dict[str, str]]:
    """Spark JDBC URL and connection properties for the engine's database (the same
    database ``swap_in`` runs on): PostgreSQL/TimescaleDB, or SQLite in tests."""
    url = engine.url
    if url.get_backend_name() == "sqlite":
        return f"jdbc:sqlite:{url.database}", {"driver": "org.sqlite.JDBC"}
    props = {"driver": "org.postgresql.Driver", "user": url.username or "",
             "password": url.password or ""}
    return f"jdbc:postgresql://{url.host}:{url.port or 5432}/{url.database}", props


def publish_spark(cfg: PlatformConfig, frames: dict, engine=None) -> list[str]:
    """Distributed publish (REQ-SRV-01): each Spark DataFrame is loaded in parallel
    into its own staging table over JDBC, then swapped in (``swap_in``) in one
    driver transaction. A failed load or swap drops the staging table and leaves the
    live table as it was. Returns the tables published."""
    from sqlalchemy import text

    eng = engine or get_engine(cfg)
    schema = cfg.timescale.get("schema")
    url, props = jdbc_target(eng)
    if schema:
        with eng.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    published = []
    for name, df in frames.items():
        if df is None or df.isEmpty():
            continue
        staging = staging_name(name)
        target = f"{schema}.{staging}" if schema else staging
        try:
            (df.write.mode("overwrite")
               .option("batchsize", "5000").option("numPartitions", "4")
               .jdbc(url, target, properties=props))
            key_cols = tuple(c for c in KEY_COLUMNS if c in df.columns)
            with eng.begin() as conn:
                swap_in(conn, name, staging, schema, key_cols)
        except Exception:
            with eng.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {_qualified(staging, schema)}"))
            raise
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
