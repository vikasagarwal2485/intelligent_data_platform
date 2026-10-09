"""Call every MCP analytics tool as the configured MCP identity against Trino.

``make mcp-smoke TENANT=<t>`` (``python -m idp.mcp.smoke``): with ``make trino-api``
and ``make keycloak-api`` running, this checks what a Claude Desktop session for
that tenant would get. It prints one line per tool, then checks that every other
configured tenant is refused (REQ-MCP-06), and exits non-zero if any tool returns
an error or another tenant is not refused.
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
    from idp.mcp import auth

    cfg = load_config()
    identity = auth.provider(cfg)
    if identity is None:
        print("MCP authentication is not configured: set IDP_MCP_CLIENT_ID / IDP_MCP_CLIENT_SECRET "
              "(make mcp-smoke TENANT=<tenant> does)", file=sys.stderr)
        return 2
    try:
        own = identity.tenant
    except auth.IdentityError as e:
        print(f"FAIL no token: {e} (source: {e.source})")
        return 1
    print(f">> Trino at {sql.endpoint(cfg)} (catalog {cfg.trino['catalog']}) as mcp@{own}, "
          f"token from {identity.source}")
    failures = 0
    for name, fn in tools.TOOLS.items():
        start = time.monotonic()
        out = fn()
        ms = (time.monotonic() - start) * 1000
        if "error" in out:
            failures += 1
            print(f"FAIL {own} {name}: {out['error']} (source: {out.get('source')})")
        else:
            print(f"ok   {own} {name}: {_HEADLINE[name](out)} ({ms:.0f} ms)")
    for other in [t.id for t in cfg.tenants if t.id != own]:
        out = tools.analyze_subscribers_tool(other)
        refused = "action_required" in out and out.get("available_tenants") == [own]
        failures += 0 if refused else 1
        print(f"{'ok  ' if refused else 'FAIL'} {other} refused for mcp@{own}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
