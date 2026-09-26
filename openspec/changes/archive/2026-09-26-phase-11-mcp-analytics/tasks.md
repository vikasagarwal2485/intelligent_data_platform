# Phase 11 — Tasks

Implement in order. Tick each box (`- [ ]` → `- [x]`) as completed. `make test`
must stay green after each group.

## B. Synthetic data (approved 2026-07-19 — REQ-SYN-04)
- [x] Add a price book + `category`/`monthly_fee` to generated `products`
- [x] Diversify `recharge_type` (`manual`/`auto-renewal`/`promo`/`voucher-topup`)
- [x] Generate a `fee_charges` fact (recurring/one-time/overage/discount) per subscriber
- [x] Add `fee_charges` to `SyntheticDataset` (+ `event_tables`) and the CLI generate/produce paths
- [x] Draw the additive fields from a separate `aux_rng` so labels/models stay byte-identical
- [x] Update `specs/synthetic-data/spec.md` with `REQ-SYN-04` + scenarios
- [x] Test: referential integrity of fee_charges (product_id/agreement_id exist); determinism

## C. Silver — fact_fee_charge
- [x] `build_fact_fee_charge()` in `curation/silver.py` (tenant-scoped, tokenized `service_id`, money normalized)
- [x] Add `fact_fee_charge` to `curate()` output + `SILVER_TABLES`
- [x] Wire landing → Bronze → Silver (ENTITIES) + streaming producer/consumer map + `fees` topic + Avro schema
- [x] Update `specs/silver-curation/spec.md` (add `REQ-SLV-05` / `fact_fee_charge`)
- [x] Test: `fact_fee_charge` built, `service_id` tokenized, no raw MSISDN

## A. MCP tools (REQ-MCP-01…05)
- [x] `src/idp/mcp/tools.py`: `analyze_subscribers_tool` (REQ-MCP-01)
- [x] `analyze_usage_events_tool` (REQ-MCP-02)
- [x] `analyze_recharges_tool` (REQ-MCP-03)
- [x] `analyze_recharge_anomalies_tool` (REQ-MCP-04)
- [x] `analyze_fee_charges_tool` (REQ-MCP-05)
- [x] Each tool: tenant-scoped Silver read via `storage` helpers; JSON-able dict out; no raw PII
- [x] Tests: one per tool asserting the documented keys + tenant isolation (REQ-MCP-06)

## A. MCP server + wiring (REQ-MCP-06)
- [x] `src/idp/mcp/server.py`: register all five tools on a FastMCP stdio server
- [x] `mcp` dependency in `pyproject.toml` + `requirements.txt`
- [x] `idp mcp` CLI subcommand
- [x] `make mcp` target
- [x] Test: registry has five tools; server builds (importorskip mcp)

## Docs / traceability
- [x] `roadmap.md` row 11; `README.md` capability-code table (+`MCP`)
- [x] `decisions/decision-log.md`: ADR-0021, ADR-0022
- [x] Regenerate `python docs/openspec/gen_traceability.py`
- [x] `make test` green (except pre-existing `test_serving_timescale` needing sqlalchemy in-env)
