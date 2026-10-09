"""The compose Trino stack's identities, for the ``trino``-marked tests.

``make trino-stack`` generates the stack's secrets into ``deploy/dev/trino/.secrets``:
a test key pair whose public half the stack's Trino trusts instead of Keycloak's
(``deploy/dev/trino/config.properties``), the service users' passwords, and the
TLS certificate. Tests sign tokens with the private key: valid tenant principals
and forged ones (REQ-TEN-04).
"""
from __future__ import annotations

import contextlib
import time
from pathlib import Path

import jwt
import numpy as np
import pandas as pd
import pytest

from idp.common.config import load_config
from idp.mcp import sql

SECRETS = Path(__file__).resolve().parents[1] / "deploy" / "dev" / "trino" / ".secrets"
ISSUER = "https://idp-test.invalid/realms/idp"     # = the stack's required issuer
HTTPS_PORT = 18443
TENANTS = ("tenant_a", "tenant_b")


def private_key() -> str:
    return (SECRETS / "jwt-private.pem").read_text()


def ca_cert() -> str:
    return str(SECRETS / "tls.crt")


def password(user: str) -> str:
    env = dict(line.split("=", 1) for line in (SECRETS / "passwords.env").read_text().split())
    return env[f"TRINO_{user.upper()}_PASSWORD"]


def token(principal: str | None = "mcp@tenant_a", *, tenant: str | None = None, key: str | None = None,
          audience: str = "trino", ttl: int = 300, issuer: str = ISSUER) -> str:
    """A signed access token; ``principal=None`` omits ``idp_principal``."""
    now = int(time.time())
    claims = {"iss": issuer, "aud": audience, "iat": now, "exp": now + ttl,
              "tenant_id": tenant or (principal or "@").split("@")[-1]}
    if principal is not None:
        claims["idp_principal"] = principal
    return jwt.encode(claims, key or private_key(), algorithm="RS256")


class TenantIdentity:
    """A token provider for ``mcp@<tenant>`` signed with the stack's test key."""

    source = "test signer (deploy/dev/trino/.secrets)"

    def __init__(self, tenant: str) -> None:
        self.tenant = tenant

    def token(self) -> str:
        return token(f"mcp@{self.tenant}")


@contextlib.contextmanager
def as_tenant(tenant: str):
    """Run the MCP tools (and ``sql.query``) as ``mcp@<tenant>``."""
    from idp.mcp import auth
    auth.use_provider(TenantIdentity(tenant))
    try:
        yield
    finally:
        auth.use_provider(None)


def connect(*, principal: str | None = None, raw_token: str | None = None, user: str | None = None,
            password_of: str | None = None, port: int = HTTPS_PORT, scheme: str = "https"):
    """A DB-API connection to the stack's ``lake`` catalog: as a token (``principal``
    or ``raw_token``), as a service user with its password (``password_of``,
    impersonating ``user`` if given), or with no credential at all."""
    import trino
    from trino.auth import BasicAuthentication, JWTAuthentication

    auth = None
    if raw_token or principal:
        auth = JWTAuthentication(raw_token or token(principal))
    elif password_of:
        auth = BasicAuthentication(password_of, password(password_of))
    return trino.dbapi.connect(host="127.0.0.1", port=port, http_scheme=scheme, auth=auth,
                               user=user, catalog="lake", verify=ca_cert(), timezone="UTC")


def rows(conn, sql: str) -> list:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# The shared two-tenant lake in the stack                                      #
# --------------------------------------------------------------------------- #
# The compose stack's host ports; the fixture wipes that lake, so it refuses to
# run against anything else (e.g. a port-forward to the cluster).
STACK = {"trino": HTTPS_PORT, "catalog": "127.0.0.1:15432/iceberg", "s3": "http://127.0.0.1:19000"}
LAYERS = ("bronze", "silver", "gold")


def _stack_cfg():
    cfg = load_config()
    if not (cfg.trino["port"] == STACK["trino"] and STACK["catalog"] in cfg.catalog["uri"]
            and cfg.s3.get("endpoint") == STACK["s3"] and cfg.catalog.get("name") == "lake"):
        pytest.skip("needs the compose Trino stack: run `make mcp-trino-test`")
    try:
        with as_tenant("tenant_a"):
            sql.query(cfg, "SELECT 1 AS tenant_id")
    except (sql.LakeReadError, FileNotFoundError) as e:
        pytest.skip(f"compose Trino not reachable ({e}); `make trino-stack` starts it")
    return cfg


def _wipe(cfg) -> None:
    """Empty the compose lake: its buckets and every table's catalog row (the
    namespaces stay registered)."""
    import s3fs
    import sqlalchemy as sa

    fs = s3fs.S3FileSystem(key=cfg.s3["access_key"], secret=cfg.s3["secret_key"],
                           client_kwargs={"endpoint_url": cfg.s3["endpoint"]})
    for layer in LAYERS:
        bucket = f"{cfg.s3['bucket_prefix']}-{layer}"
        if fs.exists(bucket):
            fs.rm(bucket, recursive=True)
        fs.mkdir(bucket)
    engine = sa.create_engine(cfg.catalog["uri"])
    with engine.begin() as conn:
        if sa.inspect(conn).has_table("iceberg_tables"):
            conn.execute(sa.text("DELETE FROM iceberg_tables"))
    engine.dispose()


def _scores(tenant: str, n: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gold score history: ``n`` rows per table with ``scored_at`` spread over ~40
    days, so the 7/15/30-day windows differ."""
    rng = np.random.default_rng(seed)
    now = pd.Timestamp.now(tz="UTC").floor("s")
    at = [now - pd.Timedelta(days=int(d)) for d in rng.integers(0, 40, n)]
    ids = [f"tok_{tenant}_{i}" for i in range(n)]
    fraud = pd.DataFrame({
        "tenant_id": tenant, "service_id": ids,
        "fraud_proba": rng.beta(1, 6, n), "rule_score": rng.choice([0.0, 0.2, 0.4, 0.6, 1.0], n),
        "fraud_score": rng.beta(1, 5, n), "scored_at": at})
    afford = rng.gamma(4, 25, n)
    credit = pd.DataFrame({
        "tenant_id": tenant, "service_id": ids, "pd": rng.beta(2, 8, n),
        "affordability_base": afford, "recommended_limit": afford * rng.uniform(0.4, 0.95, n),
        "scored_at": at})
    return fraud, credit


@pytest.fixture(scope="session")
def trino_lake(spark):
    """A two-tenant lake in the compose stack, built through the real stages."""
    from idp.common import catalog
    from idp.ingestion.raw import raw_events
    from idp.pipeline import to_bronze, to_silver
    from idp.synthetic.generator import generate_for_tenants

    cfg = _stack_cfg()
    _wipe(cfg)
    ds = generate_for_tenants(list(TENANTS), subscribers=150, days=60, fraud_rate=0.2, seed=11)
    to_bronze(cfg, raw_events(ds), persist=True)
    to_silver(cfg, full_refresh=True)
    for i, t in enumerate(TENANTS):
        fraud, credit = _scores(t, 400 + 100 * i, seed=i)
        catalog.append_table(cfg, "gold", "fraud_scores", fraud)
        catalog.append_table(cfg, "gold", "credit_scores", credit)
    return cfg
