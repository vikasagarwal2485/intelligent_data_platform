# Phase 11 — MCP Analytics Service (+ synthetic fee-charge data)

Date: 2026-07-19

## Why
The medallion lake already curates subscribers, usage, recharges and (new) fee
charges into tenant-scoped Silver tables, but there is no natural-language way to
interrogate them. This phase exposes the lake to AI agents via **five read-only,
tenant-scoped MCP tools** — one per business question set — so an analyst (or
Claude) can ask "what's the roamer split for tenant_a?" or "top products by fee
revenue this quarter?" and get compact, aggregated JSON back.

Four of the five tools map onto existing Silver (`dim_subscriber`, `fact_usage`,
`fact_recharge`). The fifth — **fee charges** — has **no source data today**
(`products` carries no price/fee and there is no fee-charge event stream), so this
phase also **enhances the synthetic generator** (proposal below, for review). Two
smaller data gaps are closed at the same time: the roamer flag (derived from usage)
and `recharge_type`, which is currently the constant `"manual"`.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).

### A. MCP analytics service (`REQ-MCP-01…06`)
- New `src/idp/mcp/` package: `tools.py` (five pure analytic functions over Silver,
  returning JSON-able dicts) + `server.py` (an MCP server registering them) —
  `REQ-MCP-01…06`.
- `analyze_subscribers_tool` — subscription vs device counts, business-line mix,
  roamer split — `REQ-MCP-01`.
- `analyze_usage_events_tool` — event counts, revenue/duration per event type,
  usage over time — `REQ-MCP-02`.
- `analyze_recharges_tool` — total/avg/count top-ups, splits by recharge_type /
  payment_method / time — `REQ-MCP-03`.
- `analyze_recharge_anomalies_tool` — amount & volume spikes, subscriber-day
  anomalies, weekly/monthly views — `REQ-MCP-04`.
- `analyze_fee_charges_tool` — product fee revenue, revenue distribution, charge-type
  analysis, product/category/month/quarter breakdowns — `REQ-MCP-05`.
- New CLI subcommand `idp mcp` starts the stdio MCP server — `REQ-MCP-06`.
- `mcp` runtime dependency (Python MCP SDK / FastMCP) in `pyproject.toml` /
  `requirements.txt`.
- `make mcp` target + docs; optional in-cluster Deployment deferred to a follow-up.

### B. Synthetic fee-charge data + two gap fixes (`REQ-SYN-04`, supports `REQ-MCP-03/05`)
> **This is the part flagged for review/approval.** See "Synthetic data proposal".
- New `fee_charges` entity in `synthetic/generator.py`: recurring/one-time/overage/
  discount product charges per subscriber over the window — `REQ-SYN-04`.
- Enrich `products` with `category` and `monthly_fee` so revenue distribution is
  meaningful — `REQ-SYN-04`.
- Diversify `recharge_type` from the constant `"manual"` to a realistic mix
  (`manual`/`auto-renewal`/`promo`/`voucher-topup`) so the recharge_type split is
  non-degenerate — supports `REQ-MCP-03`.

### C. Silver: fee-charge fact (`SLV` delta, supports `REQ-MCP-05`)
- New `build_fact_fee_charge()` in `curation/silver.py` → `fact_fee_charge`
  (tenant-scoped, `service_id` tokenized, money normalized), added to `curate()`.
- Landing/Bronze/streaming wiring so the new entity flows generate → Bronze → Silver.

## Synthetic data proposal (for review)
Adding one fact table and enriching two existing ones. All deterministic under the
existing per-tenant seed; existing tables/columns are unchanged (additive only), so
current tests and pipelines keep passing.

**1. New `fee_charges` fact (the data behind Tool 5).** One row per product charge
event:

| column | example | notes |
|---|---|---|
| `tenant_id` | `tenant_a` | isolation column |
| `id` | `tenant_a-agr-000001-fc-3` | unique |
| `service_id` | `595912345678` | MSISDN (tokenized in Silver) |
| `agreement_id` | `tenant_a-agr-000001` | FK |
| `product_id` | `tenant_a-prod-000001-0` | FK to products |
| `product_name` | `Data 5 GB` | denormalized for convenience |
| `category` | `subscription` | `subscription`/`addon`/`device`/`roaming`/`discount` |
| `charge_type` | `recurring` | `recurring`/`one-time`/`overage`/`discount` |
| `amount` | `19.99` | negative for `discount` |
| `currency` | `EUR` | |
| `charged_at` | `2026-02-01T00:00:00Z` | monthly cadence for recurring |

Generation logic: each subscriber's products emit a **monthly recurring** charge
(from the product's `monthly_fee`), plus occasional **one-time** (device/activation)
and **overage** charges, and a small fraction get a **discount** (negative amount).
Amounts scale with the subscriber's existing latent `ability` so revenue correlates
with behaviour.

**2. `products` enrichment.** Add `category` (`subscription`/`addon`/`device`/
`roaming`) and `monthly_fee` (derived from the product name/catalog). Backfills the
existing `PRODUCT_NAMES` with a small price book.

**3. `recharge_type` mix.** Replace the constant `"manual"` with a weighted draw
over `{manual, auto-renewal, promo, voucher-topup}`.

**Scope boundary:** no schema change to labels, credit/fraud features, or the ML
models — this is purely additive analytics data. Fee charges are **not** fed into
the credit/fraud models in this phase (could be a later feature-engineering change).

## Impact
- Spec deltas: `mcp-analytics` (ADDED, new capability), `synthetic-data` (ADDED
  `REQ-SYN-04`), `silver-curation` (ADDED `fact_fee_charge`).
- Code: `src/idp/mcp/**`, `src/idp/synthetic/generator.py`,
  `src/idp/curation/silver.py`, `src/idp/curation/jsonapi.py` (if streamed),
  `src/idp/ingestion/**`, `src/idp/streaming/**`, `src/idp/cli.py`,
  `pyproject.toml`, `requirements.txt`, `Makefile`, `tests/**`.
- Decisions: ADR-0021 (MCP analytics service), ADR-0022 (synthetic fee-charge data).

## Spec Deltas
Satisfies every requirement in `specs/mcp-analytics` and `REQ-SYN-04` in
`specs/synthetic-data`, and adds `fact_fee_charge` to `specs/silver-curation`.

## Open decisions (confirm before implementation)
1. **Synthetic fee-charge model** above — approve as-is or adjust scope.
2. **MCP transport** — default: stdio server via `idp mcp` (works with Claude
   Desktop / `mcp` clients); an in-cluster streamable-HTTP Deployment can follow.
3. **Tool data source** — default: pandas over the Silver lake (MinIO/local) via
   the existing `storage` helpers (matches the prototype, unit-testable without a
   cluster), rather than live Trino SQL.
