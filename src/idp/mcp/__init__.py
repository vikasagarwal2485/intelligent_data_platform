"""MCP analytics surface over the Silver lake (REQ-MCP-01…06).

``tools`` holds the five read-only, tenant-scoped analytic functions; ``server``
exposes them over the Model Context Protocol (stdio). See ADR-0021.
"""
from idp.mcp.tools import TOOLS

__all__ = ["TOOLS"]
