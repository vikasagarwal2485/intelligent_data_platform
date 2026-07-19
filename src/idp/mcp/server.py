"""MCP server exposing the five Silver analytics tools over stdio (REQ-MCP-06).

Run with ``idp mcp`` (or ``python -m idp.mcp.server``). Every tool is read-only,
requires a ``tenant_id`` and returns tenant-scoped, tokenized aggregates — see
``tools.py`` and ADR-0021. The heavy ``mcp`` dependency is imported lazily so the
rest of the package (and the tool functions, used directly in tests) need no MCP
install.
"""
from __future__ import annotations

from typing import Any

from idp.mcp import tools


def build_server(config_path: str | None = None):
    """Construct a FastMCP server with all five analytics tools registered."""
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("idp-analytics")

    @server.tool()
    def analyze_subscribers_tool(tenant_id: str) -> dict[str, Any]:
        """Subscriber base: subscription vs device counts, business-line mix, roamer split."""
        return tools.analyze_subscribers_tool(tenant_id, config_path=config_path)

    @server.tool()
    def analyze_usage_events_tool(tenant_id: str, granularity: str = "month") -> dict[str, Any]:
        """Usage events: total count, revenue & duration per event type, usage over time."""
        return tools.analyze_usage_events_tool(tenant_id, granularity=granularity, config_path=config_path)

    @server.tool()
    def analyze_recharges_tool(tenant_id: str, granularity: str = "month") -> dict[str, Any]:
        """Recharges: total top-up, count, average, splits by type/payment-method/time."""
        return tools.analyze_recharges_tool(tenant_id, granularity=granularity, config_path=config_path)

    @server.tool()
    def analyze_recharge_anomalies_tool(
        tenant_id: str, sensitivity: float = 3.0, view: str = "monthly"
    ) -> dict[str, Any]:
        """Recharge anomalies: amount/volume spikes, subscriber-day outliers, periodic views."""
        return tools.analyze_recharge_anomalies_tool(
            tenant_id, sensitivity=sensitivity, view=view, config_path=config_path
        )

    @server.tool()
    def analyze_fee_charges_tool(tenant_id: str, breakdown: str = "month") -> dict[str, Any]:
        """Fee charges: product fee revenue, revenue distribution, charge-type & period breakdowns."""
        return tools.analyze_fee_charges_tool(tenant_id, breakdown=breakdown, config_path=config_path)

    return server


def main(config_path: str | None = None) -> None:
    """Start the stdio MCP server (blocking)."""
    build_server(config_path).run()


if __name__ == "__main__":
    main()
