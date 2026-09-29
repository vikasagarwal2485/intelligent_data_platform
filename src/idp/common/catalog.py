"""The lake's Iceberg catalog (REQ-FND-04): table resolution for Spark and Python.

Bronze/Silver/Gold are Iceberg tables in namespaces ``bronze``/``silver``/``gold``.
One catalog configuration (``cfg.catalog``) serves both engines:

* ``type: sql`` — an Iceberg JDBC catalog that ``pyiceberg`` (SqlCatalog) and Spark
  (Iceberg JdbcCatalog) share. Locally and in tests it is a SQLite file
  (``<storage.root>/catalog.db``, no services needed); in the cluster it is the
  ``iceberg`` database on the platform Postgres (TimescaleDB). Both keep every
  table's full snapshot history, which the incremental Silver streams and the
  snapshot-expiry guard rely on.
* ``type: rest`` — any Iceberg REST catalog. (Not used in the cluster: Nessie's
  REST catalog keeps only a table's current snapshot, see ADR-0028.)

A table's data lives under its layer's location, ``<lake_uri(layer)>/tables/<table>``
(``tables/`` keeps Iceberg data apart from any legacy Parquet at
``<lake_uri(layer)>/<table>`` — orphan-file cleanup never touches it).

This module is the Python side (landing, ML/serving/analytics reads); the Spark
side configures the same catalog in ``idp.transforms.session``.
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa

from idp.common import storage
from idp.common.config import PlatformConfig

LAYERS = ("bronze", "silver", "gold")
LOCAL_TABLE_DIR = "tables"


# --------------------------------------------------------------------------- #
# configuration shared by both engines                                        #
# --------------------------------------------------------------------------- #
def catalog_name(cfg: PlatformConfig) -> str:
    """Catalog name used by Spark, pyiceberg AND Trino (it is stored in every
    JDBC-catalog row, and Trino's ``catalog-name`` must match it; SCN-FND-01.7).

    ``catalog.name`` / ``IDP_CATALOG_NAME`` wins. Otherwise a shared catalog (the
    platform Postgres, REST) is ``lake``, independent of its URI and credentials; a
    local SQLite catalog gets a per-file name so one process can hold several local
    lakes (e.g. per-test temp lakes).
    """
    c = cfg.catalog
    if c.get("name"):                                 # catalog.name / IDP_CATALOG_NAME
        return c["name"]
    if c["type"] == "sql" and is_sqlite(c["uri"]):
        return "lake_" + hashlib.sha1(c["uri"].encode()).hexdigest()[:10]
    return "lake"                                     # a stable name Trino can address


def table_location(cfg: PlatformConfig, layer: str, table: str) -> str:
    """Where a table's data and metadata live (its layer's bucket or local dir)."""
    base = storage.lake_uri(cfg, layer)
    if base.startswith("s3://"):
        return f"{base}/{LOCAL_TABLE_DIR}/{table}"
    return (Path(base).resolve() / LOCAL_TABLE_DIR / table).as_uri()


def _s3_properties(cfg: PlatformConfig) -> dict[str, str]:
    s3 = cfg.s3
    if not s3.get("enabled"):
        return {}
    return {
        "s3.endpoint": str(s3["endpoint"]),
        "s3.access-key-id": str(s3["access_key"]),
        "s3.secret-access-key": str(s3["secret_key"]),
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }


def is_sqlite(uri: str) -> bool:
    return uri.startswith("sqlite:///")


def jdbc_connection(uri: str) -> dict[str, str]:
    """Spark JdbcCatalog settings for a SQLAlchemy catalog URI (SQLite or Postgres)."""
    if is_sqlite(uri):
        return {"uri": "jdbc:sqlite:" + sqlite_path(uri)}
    from sqlalchemy.engine import make_url
    url = make_url(uri)
    if not url.drivername.startswith("postgresql"):
        raise ValueError(f"unsupported SQL catalog database {url.drivername!r}; use sqlite or postgresql")
    conn = {"uri": f"jdbc:postgresql://{url.host}:{url.port or 5432}/{url.database}"}
    if url.username:
        conn["jdbc.user"] = url.username
    if url.password:
        conn["jdbc.password"] = url.password
    return conn


def pyiceberg_properties(cfg: PlatformConfig) -> dict[str, str]:
    c = cfg.catalog
    props: dict[str, str] = {"uri": c["uri"], **_s3_properties(cfg)}
    if c["type"] == "sql":
        props["warehouse"] = c["warehouse"]
        if is_sqlite(c["uri"]):
            props["connect_args.timeout"] = "60"      # wait on SQLite's single writer
    else:
        props["type"] = "rest"
        if c.get("warehouse"):
            props["warehouse"] = c["warehouse"]
    return props


def spark_catalog_conf(cfg: PlatformConfig) -> dict[str, str]:
    """``spark.sql.catalog.<name>.*`` settings for the same catalog (task-1.1 spike)."""
    c = cfg.catalog
    p = f"spark.sql.catalog.{catalog_name(cfg)}"
    conf = {
        p: "org.apache.iceberg.spark.SparkCatalog",
        # every stage must read the latest committed snapshot, not a cached one
        f"{p}.cache-enabled": "false",
    }
    if c["type"] == "sql":
        conf.update({f"{p}.{k}": v for k, v in jdbc_connection(c["uri"]).items()})
        conf.update({
            f"{p}.type": "jdbc",
            f"{p}.warehouse": c["warehouse"],
            # pyiceberg creates the catalog tables; the JDBC init check would otherwise
            # hold a SQLite read transaction open and hide later external commits
            f"{p}.jdbc.init-catalog-tables": "false",
        })
    else:
        conf.update({f"{p}.type": "rest", f"{p}.uri": c["uri"]})
        if c.get("warehouse"):
            conf[f"{p}.warehouse"] = c["warehouse"]
    if cfg.s3.get("enabled"):
        conf.update({
            f"{p}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"{p}.s3.endpoint": str(cfg.s3["endpoint"]),
            f"{p}.s3.path-style-access": "true",
            f"{p}.s3.access-key-id": str(cfg.s3["access_key"]),
            f"{p}.s3.secret-access-key": str(cfg.s3["secret_key"]),
            f"{p}.client.region": "us-east-1",
        })
    return conf


def spark_table(cfg: PlatformConfig, layer: str, table: str) -> str:
    """Fully qualified Spark SQL identifier of a lake table."""
    return f"{catalog_name(cfg)}.{layer}.{table}"


# --------------------------------------------------------------------------- #
# pyiceberg catalog                                                           #
# --------------------------------------------------------------------------- #
_CATALOGS: dict[tuple, Any] = {}


def sqlite_path(uri: str) -> str:
    """Filesystem path of a ``sqlite:///<path>`` catalog URI."""
    if not uri.startswith("sqlite:///"):
        raise ValueError(f"not a SQLite catalog URI: {uri!r}")
    return uri[len("sqlite:///"):]


def _prepare_sqlite(uri: str) -> None:
    """Create the catalog DB file in WAL mode (readers never block the writer)."""
    path = Path(sqlite_path(uri))
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    try:
        con.execute("PRAGMA journal_mode=WAL")
    finally:
        con.close()


def load_catalog(cfg: PlatformConfig):
    """The pyiceberg catalog for ``cfg`` (cached per configuration)."""
    props = pyiceberg_properties(cfg)
    key = (catalog_name(cfg), tuple(sorted(props.items())))
    if key not in _CATALOGS:
        if cfg.catalog["type"] == "sql":
            from pyiceberg.catalog.sql import SqlCatalog
            if is_sqlite(cfg.catalog["uri"]):
                _prepare_sqlite(cfg.catalog["uri"])
            cat = SqlCatalog(catalog_name(cfg), **props)
        else:
            from pyiceberg.catalog import load_catalog as _load
            cat = _load(catalog_name(cfg), **props)
        ensure_namespaces(cat, cfg)
        _CATALOGS[key] = cat
    return _CATALOGS[key]


def namespace_location(cfg: PlatformConfig, layer: str) -> str:
    """Base location of a layer's tables: ``<lake_uri(layer)>/tables``."""
    base = storage.lake_uri(cfg, layer)
    if base.startswith("s3://"):
        return f"{base}/{LOCAL_TABLE_DIR}"
    return (Path(base).resolve() / LOCAL_TABLE_DIR).as_uri()


def ensure_namespaces(cat, cfg: PlatformConfig) -> None:
    """Create the layer namespaces, each with its ``location`` property set.

    The JDBC catalog honours each table's explicit location; the namespace
    property is the default for catalogs that don't (some REST catalogs place a
    new table at ``<namespace location>/<table>_<uuid>``), so every layer's tables
    stay in its own bucket, under ``tables/``.
    """
    existing = {ns[0] for ns in cat.list_namespaces()}
    for layer in LAYERS:
        location = namespace_location(cfg, layer)
        if layer not in existing:
            cat.create_namespace_if_not_exists(layer, {"location": location})
        elif cat.load_namespace_properties(layer).get("location") != location:
            cat.update_namespace_properties(layer, updates={"location": location})


def table_exists(cfg: PlatformConfig, layer: str, table: str) -> bool:
    return load_catalog(cfg).table_exists(f"{layer}.{table}")


# --------------------------------------------------------------------------- #
# Python reads and writes                                                     #
# --------------------------------------------------------------------------- #
def read_table(cfg: PlatformConfig, layer: str, table: str, row_filter=None,
               columns: Iterable[str] | None = None) -> pd.DataFrame:
    """A lake table as pandas, with ``row_filter`` pushed into the table scan.

    ``row_filter`` is a pyiceberg expression (e.g. ``EqualTo("tenant_id", "t")``)
    or a string; files outside the matching partitions are not read (SCN-FND-04.4).
    Raises ``pyiceberg.exceptions.NoSuchTableError`` when the table was never written.
    """
    t = load_catalog(cfg).load_table(f"{layer}.{table}")
    kwargs: dict[str, Any] = {}
    if row_filter is not None:
        kwargs["row_filter"] = row_filter
    if columns is not None:
        kwargs["selected_fields"] = tuple(columns)
    df = t.scan(**kwargs).to_pandas()
    if "tenant_id" in df.columns:
        df["tenant_id"] = df["tenant_id"].astype(str)
    return df


def _normalize_arrow(tbl: pa.Table) -> pa.Table:
    """Arrow types Iceberg can hold: all-null columns become strings."""
    fields = []
    for f in tbl.schema:
        fields.append(pa.field(f.name, pa.string()) if pa.types.is_null(f.type) else f)
    return tbl.cast(pa.schema(fields))


def create_or_evolve(cfg: PlatformConfig, layer: str, table: str, arrow: pa.Table,
                     partition_by: Iterable[str] = ("tenant_id",)):
    """Create the table (location in its layer) or add any new columns from
    ``arrow`` (union by name; columns are never dropped). ``partition_by`` holds
    column names (identity) or ``"day:<timestamp column>"``."""
    cat = load_catalog(cfg)
    ident = f"{layer}.{table}"
    if not cat.table_exists(ident):
        t = cat.create_table(ident, schema=arrow.schema, location=table_location(cfg, layer, table))
        parts = [p for p in partition_by if p.split(":")[-1] in arrow.column_names]
        if parts:
            from pyiceberg.transforms import DayTransform
            with t.update_spec() as spec:
                for part in parts:
                    if part.startswith("day:"):            # e.g. "day:scored_at"
                        col = part.split(":", 1)[1]
                        spec.add_field(col, DayTransform(), f"{col}_day")
                    else:
                        spec.add_identity(part)
        return cat.load_table(ident)
    t = cat.load_table(ident)
    if set(arrow.column_names) - {f.name for f in t.schema().fields}:
        with t.update_schema() as upd:
            upd.union_by_name(arrow.schema)
        t = cat.load_table(ident)
    return t


def _conform(arrow: pa.Table, t) -> pa.Table:
    """Reorder/cast ``arrow`` to the table schema, null-filling absent columns."""
    target = t.schema().as_arrow()
    cols = []
    for f in target:
        if f.name in arrow.column_names:
            cols.append(arrow[f.name].cast(f.type))
        else:
            cols.append(pa.nulls(arrow.num_rows, type=f.type))
    return pa.Table.from_arrays(cols, schema=target)


def append_table(cfg: PlatformConfig, layer: str, table: str, df: pd.DataFrame,
                 partition_by: Iterable[str] = ("tenant_id",)) -> int:
    """Append ``df`` as one snapshot (creating/evolving the table first)."""
    if df is None or df.empty:
        return 0
    arrow = _normalize_arrow(pa.Table.from_pandas(df, preserve_index=False))
    t = create_or_evolve(cfg, layer, table, arrow, partition_by)
    t.append(_conform(arrow, t))
    return arrow.num_rows


def overwrite_tenants(cfg: PlatformConfig, layer: str, table: str, df: pd.DataFrame,
                      partition_by: Iterable[str] = ("tenant_id",)) -> int:
    """Atomically replace the rows of the tenants present in ``df`` (one snapshot)."""
    if df is None or df.empty:
        return 0
    from pyiceberg.expressions import In
    arrow = _normalize_arrow(pa.Table.from_pandas(df, preserve_index=False))
    t = create_or_evolve(cfg, layer, table, arrow, partition_by)
    tenants = sorted(set(df["tenant_id"].astype(str)))
    t.overwrite(_conform(arrow, t), overwrite_filter=In("tenant_id", tenants))
    return arrow.num_rows


def append_chunks(cfg: PlatformConfig, layer: str, table: str, schema: pa.Schema,
                  chunks: Iterable[pa.Table], partition_by: Iterable[str] = ("tenant_id",),
                  properties: dict[str, str] | None = None) -> int:
    """Append every chunk as ONE snapshot, and set ``properties``, in one commit.

    ``schema`` covers all chunks (it creates or evolves the table up front), so a
    large load can be streamed chunk by chunk without a schema change mid-commit.
    """
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    t = create_or_evolve(cfg, layer, table, _normalize_arrow(schema.empty_table()), partition_by)
    rows = 0
    with t.transaction() as tx:
        # ONE append snapshot whose summary lists every chunk's files as added:
        # an incremental reader (Silver's Structured Streaming source) reads the
        # files a snapshot added, so several appends squashed into one commit
        # would hide all but the last chunk from it.
        with tx.update_snapshot().fast_append() as append_files:
            for chunk in chunks:
                data = _conform(_normalize_arrow(chunk), t)
                for data_file in _dataframe_to_data_files(table_metadata=tx.table_metadata, df=data,
                                                          io=t.io, write_uuid=append_files.commit_uuid):
                    append_files.append_data_file(data_file)
                rows += chunk.num_rows
        if properties:
            tx.set_properties(properties)
    return rows


def replace_table(cfg: PlatformConfig, layer: str, table: str, df: pd.DataFrame,
                  partition_by: Iterable[str] = ("tenant_id",)) -> int:
    """Replace the whole table's contents with ``df`` in one snapshot."""
    arrow = _normalize_arrow(pa.Table.from_pandas(df, preserve_index=False))
    t = create_or_evolve(cfg, layer, table, arrow, partition_by)
    t.overwrite(_conform(arrow, t))
    return arrow.num_rows


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
