# Phase 11 — Tasks

Implement in order. Tick each box (`- [ ]` → `- [x]`) as completed. `make test`
must stay green after each group.

## B. Synthetic data (gated on approval — REQ-SYN-04)
- [ ] Add a price book + `category`/`monthly_fee` to generated `products`
- [ ] Diversify `recharge_type` (`manual`/`auto-renewal`/`promo`/`voucher-topup`)
- [ ] Generate a `fee_charges` fact (recurring/one-time/overage/discount) per subscriber
- [ ] Add `fee_charges` to `SyntheticDataset` (+ `event_tables`) and the CLI generate/produce paths
- [ ] Update `specs/synthetic-data/spec.md` with `REQ-SYN-04` + scenarios
- [ ] Test: referential integrity of fee_charges (product_id/agreement_id exist); determinism

## C. Silver — fact_fee_charge
- [ ] `build_fact_fee_charge()` in `curation/silver.py` (tenant-scoped, tokenized `service_id`, money normalized)
- [ ] Add `fact_fee_charge` to `curate()` output
- [ ] Wire landing → Bronze → Silver for the new entity (ingestion + streaming producer/consumer + Avro if streamed)
- [ ] Update `specs/silver-curation/spec.md` (add `fact_fee_charge`)
- [ ] Test: `fact_fee_charge` built, `service_id` tokenized, no raw MSISDN

## A. MCP tools (REQ-MCP-01…05)
- [ ] `src/idp/mcp/tools.py`: `analyze_subscribers_tool` (REQ-MCP-01)
- [ ] `analyze_usage_events_tool` (REQ-MCP-02)
- [ ] `analyze_recharges_tool` (REQ-MCP-03)
- [ ] `analyze_recharge_anomalies_tool` (REQ-MCP-04)
- [ ] `analyze_fee_charges_tool` (REQ-MCP-05)
- [ ] Each tool: tenant-scoped Silver read via `storage` helpers; JSON-able dict out; no raw PII
- [ ] Tests: one per tool asserting the documented keys + tenant isolation (REQ-MCP-06)

## A. MCP server + wiring (REQ-MCP-06)
- [ ] `src/idp/mcp/server.py`: register all five tools on an MCP server (stdio)
- [ ] `mcp` dependency in `pyproject.toml` + `requirements.txt`
- [ ] `idp mcp` CLI subcommand
- [ ] `make mcp` target + `docs`/README note
- [ ] Test: server builds and advertises five tools (list_tools)

## Docs / traceability
- [ ] `roadmap.md` row 11; `project.md` capability-code table (+`MCP`)
- [ ] `decisions/decision-log.md`: ADR-0020, ADR-0021
- [ ] Regenerate `python docs/openspec/gen_traceability.py`
- [ ] `make test` green
