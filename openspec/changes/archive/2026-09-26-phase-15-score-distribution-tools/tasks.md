# Phase 15 — Tasks

## Score-distribution tools (REQ-MCP-07, REQ-MCP-08)
- [x] `analyze_fraud_score_tool` — fraud_proba / rule_score / fraud_score over 7/15/30d
- [x] `analyze_credit_score_tool` — pd / affordability_base / recommended_limit / gap over 7/15/30d
- [x] `_distribution` (count/mean/std/min/max/percentiles/histogram) helper
- [x] `_windowed_distributions` — windows vs latest `scored_at`; snapshot-fallback note
- [x] Read Gold score tables via `_read_gold`; reuse `_lake_guard` (tenant scoping)
- [x] Register both tools in `src/idp/mcp/server.py`
- [x] `pipeline.to_scores` stamps `scored_at` (UTC) per scoring run

## Tests
- [x] `test_registry_has_seven_tools`
- [x] fraud/credit per-window distribution tests (window counts, gap, isolation)
- [x] snapshot-note-without-timestamp test

## Docs / spec
- [x] `specs/mcp-analytics`: ADD REQ-MCP-07/08; UPDATE REQ-MCP-06
- [x] `roadmap.md` row 15; regenerate traceability
- [x] README §4.6 (seven-tool table + score-history caveat)
