"""MCP server exposing the seven analytics tools over stdio (REQ-MCP-06).

Run with ``idp mcp`` (or ``python -m idp.mcp.server``). Every tool is read-only and
returns tenant-scoped, tokenized aggregates, which it computes in Trino as the
server's tenant identity: its MCP client's token (``mcp.auth``; locally
``make trino-api`` and ``make keycloak-api``), row-filtered to that tenant by Trino
— see ``tools.py``, ADR-0030 and REQ-MCP-06. The server answers for that one
tenant: ``tenant_id`` is optional and any other tenant is refused. Five read Silver (subscribers/
usage/recharges/anomalies/fees); two read the Gold score tables (fraud/credit score
distributions). The heavy ``mcp`` dependency is imported lazily so the rest of the
package (and the tool functions, used directly in tests) need no MCP install.
"""
from __future__ import annotations

from typing import Any

from idp.mcp import tools


def build_server(config_path: str | None = None):
    """Construct a FastMCP server with all seven analytics tools registered."""
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("idp-analytics")

    @server.tool()
    def analyze_subscribers_tool(tenant_id: str = "") -> dict[str, Any]:
        """Subscriber base: subscription vs device counts, business-line mix, roamer split.

        tenant_id is optional: this server answers for one tenant (its own), used when
        tenant_id is omitted. If the result contains `available_tenants` and an
        `action_required`, tell the user which tenant this server serves; never guess.
        """
        return tools.analyze_subscribers_tool(tenant_id, config_path=config_path)

    @server.tool()
    def analyze_usage_events_tool(tenant_id: str = "", granularity: str = "month") -> dict[str, Any]:
        """Usage events: total count, revenue & duration per event type, usage over time.

        tenant_id is optional: this server answers for one tenant (its own), used when
        tenant_id is omitted. If the result contains `available_tenants` and an
        `action_required`, tell the user which tenant this server serves; never guess.
        """
        return tools.analyze_usage_events_tool(tenant_id, granularity=granularity, config_path=config_path)

    @server.tool()
    def analyze_recharges_tool(tenant_id: str = "", granularity: str = "month") -> dict[str, Any]:
        """Recharges: total top-up, count, average, splits by type/payment-method/time.

        tenant_id is optional: this server answers for one tenant (its own), used when
        tenant_id is omitted. If the result contains `available_tenants` and an
        `action_required`, tell the user which tenant this server serves; never guess.
        """
        return tools.analyze_recharges_tool(tenant_id, granularity=granularity, config_path=config_path)

    @server.tool()
    def analyze_recharge_anomalies_tool(
        tenant_id: str = "", sensitivity: float = 3.0, view: str = "monthly"
    ) -> dict[str, Any]:
        """Recharge anomalies: amount/volume spikes, subscriber-day outliers, periodic views.

        tenant_id is optional: this server answers for one tenant (its own), used when
        tenant_id is omitted. If the result contains `available_tenants` and an
        `action_required`, tell the user which tenant this server serves; never guess.
        """
        return tools.analyze_recharge_anomalies_tool(
            tenant_id, sensitivity=sensitivity, view=view, config_path=config_path
        )

    @server.tool()
    def analyze_fee_charges_tool(tenant_id: str = "", breakdown: str = "month") -> dict[str, Any]:
        """Fee charges: product fee revenue, revenue distribution, charge-type & period breakdowns.

        tenant_id is optional: this server answers for one tenant (its own), used when
        tenant_id is omitted. If the result contains `available_tenants` and an
        `action_required`, tell the user which tenant this server serves; never guess.
        """
        return tools.analyze_fee_charges_tool(tenant_id, breakdown=breakdown, config_path=config_path)

    @server.tool()
    def analyze_fraud_score_tool(tenant_id: str = "") -> dict[str, Any]:
        """Fraud scores: distributions of fraud probability, rule score and fused
        fraud score over the last 7/15/30 days (from the Gold `fraud_scores` table).

        tenant_id is optional: this server answers for one tenant (its own), used when
        tenant_id is omitted. If the result contains `available_tenants` and an
        `action_required`, tell the user which tenant this server serves; never guess.
        """
        return tools.analyze_fraud_score_tool(tenant_id, config_path=config_path)

    @server.tool()
    def analyze_credit_score_tool(tenant_id: str = "") -> dict[str, Any]:
        """Credit scores: distributions of probability-of-default, affordability,
        recommended limit and the affordability-vs-limit gap over the last 7/15/30
        days (from the Gold `credit_scores` table).

        tenant_id is optional: this server answers for one tenant (its own), used when
        tenant_id is omitted. If the result contains `available_tenants` and an
        `action_required`, tell the user which tenant this server serves; never guess.
        """
        return tools.analyze_credit_score_tool(tenant_id, config_path=config_path)

    return server


# Module-level instance so the standard MCP CLI can find it:
#   mcp dev src/idp/mcp/server.py      # interactive MCP Inspector (needs Node/npx)
#   mcp install src/idp/mcp/server.py  # register into Claude Desktop
# Uses the default config (config/platform.yaml); queries the Trino that config
# points at (127.0.0.1:8085 by default, i.e. `make trino-api`; IDP_TRINO_* override).
mcp = build_server()


def main(config_path: str | None = None) -> None:
    """Start the stdio MCP server (blocking)."""
    (build_server(config_path) if config_path else mcp).run()


if __name__ == "__main__":
    main()
