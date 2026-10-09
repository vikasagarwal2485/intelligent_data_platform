"""Row-filter performance check (REQ-TEN-05): the MCP tools' queries with and without
Trino's row-level security.

For each tool, its queries are recorded once (as the tenant identity), then each
query set runs ``--runs`` times, after ``--warmup`` runs, two ways, alternating so
that drift and caching affect both equally:

- **filtered**: as the tenant principal, so Trino applies the row filter;
- **unfiltered**: as ``admin``, so only the tool's own ``tenant_id = ?`` predicate
  applies.

It prints p50/p95 per tool and both ratios, and exits 1 if any tool's median
(p50) ratio exceeds ``--budget`` (1.10). The p95 ratio is reported, not gated: on a
shared host its run-to-run variance exceeds the budget (design, decision 8). The identity is the configured MCP client (``make
trino-rls-bench TENANT=<t>`` sets it, with ``make trino-api`` and ``make
keycloak-api`` running), or, for the compose test stack, tokens signed locally with
``--test-signer deploy/dev/trino/.secrets``. ``admin``'s password comes from
``TRINO_ADMIN_PASSWORD``.
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from typing import Any, Callable


def _p(values: list[float], q: float) -> float:
    return statistics.quantiles(values, n=100, method="inclusive")[int(q * 100) - 1] if len(values) > 1 else values[0]


def _signer(secrets: str, tenant: str):
    import jwt

    key = open(os.path.join(secrets, "jwt-private.pem")).read()

    class Signer:
        """Like the Keycloak provider, reuses one token until shortly before it expires."""
        source = f"test signer ({secrets})"

        def __init__(self) -> None:
            self.tenant, self._token, self._exp = tenant, "", 0

        def token(self) -> str:
            now = int(time.time())
            if now >= self._exp - 30:
                self._exp = now + 300
                self._token = jwt.encode({"iss": "https://idp-test.invalid/realms/idp", "aud": "trino",
                                          "iat": now, "exp": self._exp, "idp_principal": f"mcp@{tenant}",
                                          "tenant_id": tenant}, key, algorithm="RS256")
            return self._token
    return Signer()


def _record(tool: Callable, kwargs: dict) -> list[tuple[str, tuple]]:
    from idp.mcp import sql

    calls: list[tuple[str, tuple]] = []
    real = sql.query

    def rec(cfg, text, params=()):
        calls.append((text, tuple(params)))
        return real(cfg, text, params)
    sql.query = rec
    try:
        out = tool(**kwargs)
    finally:
        sql.query = real
    if "error" in out:
        raise RuntimeError(f"{tool.__name__}: {out['error']}")
    return [c for c in calls if c[0] != sql.TENANT_PROBE]


def _timed(connect: Callable[[], Any], queries: list[tuple[str, tuple]]) -> float:
    start = time.perf_counter()
    for text, params in queries:
        conn = connect()
        try:
            cur = conn.cursor()
            cur.execute(text, list(params))
            cur.fetchall()
        finally:
            conn.close()
    return time.perf_counter() - start


def main(argv: list[str] | None = None) -> int:
    from trino.auth import BasicAuthentication

    from idp.common.config import load_config
    from idp.mcp import auth, sql, tools

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--runs", type=int, default=60)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--budget", type=float, default=1.10, help="maximum median (p50) ratio, filtered / unfiltered")
    p.add_argument("--test-signer", help="sign tenant tokens with this secrets dir's test key (compose stack)")
    p.add_argument("--tenant", default="tenant_a", help="the tenant, with --test-signer")
    a = p.parse_args(argv)

    cfg = load_config()
    if a.test_signer:
        auth.use_provider(_signer(a.test_signer, a.tenant))
    identity = auth.provider(cfg)
    if identity is None:
        print("no MCP identity: set IDP_MCP_* (make trino-rls-bench TENANT=<t>) or --test-signer", file=sys.stderr)
        return 2
    password = os.environ.get("TRINO_ADMIN_PASSWORD")
    if not password:
        print("TRINO_ADMIN_PASSWORD is not set", file=sys.stderr)
        return 2
    t = cfg.trino

    def as_admin():
        import trino
        return trino.dbapi.connect(host=t["host"], port=int(t["port"]), catalog=t["catalog"],
                                   http_scheme=t["http_scheme"], auth=BasicAuthentication("admin", password),
                                   verify=t["ca_cert"] or True, timezone="UTC")

    print(f">> {sql.endpoint(cfg)}: mcp@{identity.tenant} (filtered) vs admin (unfiltered), "
          f"{a.runs} alternating runs after {a.warmup} warm-up, budget p50 x{a.budget:.2f}")
    print(f"{'tool':36} {'queries':>7} {'filtered p50/p95 ms':>21} {'unfiltered p50/p95 ms':>23} "
          f"{'p50 ratio':>9} {'p95 ratio':>9}")
    over = 0
    for name, tool in tools.TOOLS.items():
        queries = _record(tool, {"limit": 50} if "anomalies" in name else {})
        ways = {"filtered": lambda: sql.connect(cfg), "unfiltered": as_admin}
        for _ in range(a.warmup):
            for connect in ways.values():
                _timed(connect, queries)
        timings: dict[str, list[float]] = {k: [] for k in ways}
        for i in range(a.runs):                        # alternate, so drift and caching cancel out
            for label in (("filtered", "unfiltered") if i % 2 == 0 else ("unfiltered", "filtered")):
                timings[label].append(_timed(ways[label], queries) * 1000)
        f, u = timings["filtered"], timings["unfiltered"]
        ratio = statistics.median(f) / statistics.median(u)
        over += ratio > a.budget
        print(f"{name:36} {len(queries):>7} {_p(f, .5):>10.0f}/{_p(f, .95):<10.0f} "
              f"{_p(u, .5):>11.0f}/{_p(u, .95):<11.0f} {ratio:>9.3f} {_p(f, .95) / _p(u, .95):>9.3f}"
              f"{'  OVER' if ratio > a.budget else ''}")
    print(f">> {'all within' if not over else f'{over} tool(s) over'} the budget")
    return 1 if over else 0


if __name__ == "__main__":
    sys.exit(main())
