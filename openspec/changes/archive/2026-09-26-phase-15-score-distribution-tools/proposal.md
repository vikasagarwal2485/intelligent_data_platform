# Phase 15 — MCP score-distribution tools (fraud & credit, 7/15/30-day windows)

Date: 2026-07-22

## Why
The five phase-11 MCP tools answer questions over Silver (subscribers, usage,
recharges, anomalies, fees) but nothing surfaces the **model outputs** to an agent.
Analysts want to ask "how are fraud scores distributed this week vs last month?" and
"what does the credit book look like — PD, affordability, recommended limits, and
the haircut between them?" — per tenant, over recent windows. Those answers live in
the Gold `fraud_scores` / `credit_scores` tables written by `idp score`, not in
Silver, so two new read-only tools are added.

## What Changes
- **Two new MCP tools** (`src/idp/mcp/tools.py`, registered in `src/idp/mcp/server.py`):
  - `analyze_fraud_score_tool(tenant_id)` — distributions of `fraud_proba`,
    `rule_score` and the fused `fraud_score` over the last 7/15/30 days — `REQ-MCP-07`.
  - `analyze_credit_score_tool(tenant_id)` — distributions of `pd`,
    `affordability_base`, `recommended_limit` and the affordability-vs-limit **gap**
    over the last 7/15/30 days — `REQ-MCP-08`.
  Both read the Gold score tables, are tenant-scoped, and reuse the phase-11
  `_lake_guard` (unknown/missing tenant → ask; unreachable lake → structured error).
- **Time windows** — a compact `_distribution` (count/mean/std/min/max/percentiles/
  histogram) computed per window by `_windowed_distributions`, relative to the latest
  `scored_at`. Without accumulated timestamped history each window returns the full
  snapshot with a note — `REQ-MCP-07`/`REQ-MCP-08`.
- **`scored_at` stamp** — `pipeline.to_scores` now stamps each scoring run with a UTC
  `scored_at`, so repeated `idp score` runs accumulate the history the windows need.
- README (seven-tool table + score-history caveat) and the `mcp-analytics` spec
  updated.

## Impact
- Spec deltas: `mcp-analytics` — ADD `REQ-MCP-07`, `REQ-MCP-08`; UPDATE `REQ-MCP-06`
  ("read only that tenant's lake partition" now spans Silver + Gold score tables).
- Code: `src/idp/mcp/tools.py`, `src/idp/mcp/server.py`, `src/idp/pipeline.py`,
  `tests/test_mcp_tools.py`, `README.md`.
- Decisions: extends ADR-0021 (pandas-over-lake analytics surface) to the Gold score
  tables; no new ADR.

## Spec Deltas
Adds `REQ-MCP-07` and `REQ-MCP-08` and updates `REQ-MCP-06` in `specs/mcp-analytics`.
