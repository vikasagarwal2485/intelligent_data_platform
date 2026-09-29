"""The MCP analytics tools' query layer over Trino (REQ-MCP-06/09).

Every tool asks the lake through Trino's ``lake`` Iceberg catalog as the read-only
analytics identity (``cfg.trino["user"]``). Aggregation, filtering and ranking run
inside Trino, and only compact results come back as small pandas frames.

* **Bound parameters only.** Values (the tenant, window bounds, ``limit``,
  ``sensitivity``) are always DB-API ``?`` parameters, which Trino executes as a
  prepared statement. Table and column names come only from code constants, never
  from input.
* **Error mapping.** A table that was never written is an empty result. An
  unreachable engine, or a permission error, is a :class:`LakeReadError` naming
  the engine as ``source``, so the tools report it instead of returning a
  misleading zero.
"""
from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from idp.common.config import PlatformConfig

# Trino errors that mean "nothing written yet" rather than "cannot read".
_NOT_WRITTEN = {"TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND"}


class LakeReadError(Exception):
    """The lake could not be read (vs a table simply being absent): the query
    engine is unreachable, or the analytics identity was refused. ``source`` is the
    engine endpoint."""

    def __init__(self, message: str, source: str = "") -> None:
        super().__init__(message)
        self.source = source


def endpoint(cfg: PlatformConfig) -> str:
    t = cfg.trino
    return f"{t['http_scheme']}://{t['host']}:{t['port']}"


def connect(cfg: PlatformConfig):
    """A Trino DB-API connection to the lake catalog as the analytics identity.

    The session runs in UTC, so periods and day labels match the lake's UTC
    timestamps."""
    import trino

    t = cfg.trino
    return trino.dbapi.connect(host=t["host"], port=int(t["port"]), user=t["user"],
                               catalog=t["catalog"], http_scheme=t["http_scheme"],
                               source="idp-mcp", timezone="UTC")


def query(cfg: PlatformConfig, sql: str, params: Sequence[Any] = ()) -> pd.DataFrame:
    """Run one read query with bound parameters; the (small) result as a frame.

    An empty frame when a table isn't written yet. Raises :class:`LakeReadError`
    when Trino can't be reached or refuses the query.
    """
    import trino.exceptions as te

    source = endpoint(cfg)
    try:
        conn = connect(cfg)
        try:
            cur = conn.cursor()
            cur.execute(sql, list(params))
            rows = cur.fetchall()
            cols = [d[0] for d in (cur.description or [])]
        finally:
            conn.close()
    except te.TrinoUserError as e:
        if e.error_name in _NOT_WRITTEN:
            return pd.DataFrame()
        if e.error_name in ("PERMISSION_DENIED", "ACCESS_DENIED"):
            raise LakeReadError(f"access denied by the query engine: {e.message}", source) from e
        raise                                        # a bad query is a bug, not "no data"
    except (te.HttpError, te.TrinoConnectionError, te.TrinoExternalError, OSError) as e:
        raise LakeReadError(f"could not reach the query engine at {source}: "
                            f"{type(e).__name__}: {e}", source) from e
    except Exception as e:  # noqa: BLE001 - requests' ConnectionError & co. from the client
        if type(e).__module__.startswith(("requests", "urllib3")):
            raise LakeReadError(f"could not reach the query engine at {source}: "
                                f"{type(e).__name__}: {e}", source) from e
        raise
    return pd.DataFrame(rows, columns=cols)


def available_tenants(cfg: PlatformConfig) -> list[str]:
    """Tenants present in Silver, read from ``dim_subscriber``'s partition metadata
    only (no data files)."""
    df = query(cfg, 'SELECT DISTINCT "partition".tenant_id AS tenant_id '
                    'FROM silver."dim_subscriber$partitions"')
    if df.empty:
        return []
    return sorted(str(t) for t in df["tenant_id"].dropna().unique())
