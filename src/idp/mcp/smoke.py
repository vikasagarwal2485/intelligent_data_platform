"""Call every MCP analytics tool for every tenant against the configured Trino.

``make mcp-smoke`` (``python -m idp.mcp.smoke``): with ``make trino-api`` running,
this checks what a Claude Desktop session would get. It prints one line per tool
and tenant and exits non-zero if any call returns an error.
"""
from __future__ import annotations

import sys
import time

from idp.common.config import load_config
from idp.mcp import sql, tools

# One headline figure per tool, to show the answer is real (not an empty result).
_HEADLINE = {
    "analyze_subscribers_tool": lambda o: f"{o.get('subscriber_count')} subscribers",
    "analyze_usage_events_tool": lambda o: f"{o.get('total_events')} usage events",
    "analyze_recharges_tool": lambda o: f"{o.get('recharge_count')} recharges",
    "analyze_recharge_anomalies_tool": lambda o: (f"{len(o.get('amount_spikes', []))} amount / "
                                                  f"{len(o.get('volume_spikes', []))} volume spikes"),
    "analyze_fee_charges_tool": lambda o: f"{o.get('total_fee_revenue')} fee revenue",
    "analyze_fraud_score_tool": lambda o: f"{o.get('by_window', {}).get('30d', {}).get('count', 0)} scores in 30d",
    "analyze_credit_score_tool": lambda o: f"{o.get('by_window', {}).get('30d', {}).get('count', 0)} scores in 30d",
}


def main() -> int:
    cfg = load_config()
    print(f">> Trino at {sql.endpoint(cfg)} as {cfg.trino['user']} (catalog {cfg.trino['catalog']})")
    failures = 0
    for tenant in [t.id for t in cfg.tenants]:
        for name, fn in tools.TOOLS.items():
            start = time.monotonic()
            out = fn(tenant)
            ms = (time.monotonic() - start) * 1000
            if "error" in out:
                failures += 1
                print(f"FAIL {tenant} {name}: {out['error']} (source: {out.get('source')})")
            else:
                print(f"ok   {tenant} {name}: {_HEADLINE[name](out)} ({ms:.0f} ms)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
