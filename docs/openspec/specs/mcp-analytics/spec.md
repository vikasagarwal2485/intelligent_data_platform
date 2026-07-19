# MCP Analytics Specification

## Purpose

Expose the curated lake to AI agents as a small set of **read-only, tenant-scoped
analytics tools** over the **Model Context Protocol (MCP)**. Each tool answers a
focused business question about the Silver tables (`dim_subscriber`, `fact_usage`,
`fact_recharge`, `fact_fee_charge`) by returning compact, aggregated JSON — never
raw PII, never cross-tenant data. The tools are the natural-language analytics
surface on top of the medallion lake (ADR-0020).

Capability code: `MCP`.

## Requirements

### Requirement: REQ-MCP-01 — Subscriber analytics tool
The system SHALL provide `analyze_subscribers_tool(tenant_id, ...)` that reads
`dim_subscriber` (joined to `fact_usage` for roaming) and returns, for the tenant:
(a) subscriber count where `agreement_type = subscription`, (b) device count where
`agreement_type = device`, (c) the **business-line mix** (`mobile`/`broadband`/
`iptv`/`iot`/…), and (d) the **roamer vs non-roamer** split (a subscriber is a
roamer when they have ≥1 roaming usage event).

#### Scenario: SCN-MCP-01.1 — Agreement-type and business-line breakdown
- **WHEN** the tool runs for a tenant with generated Silver
- **THEN** it returns `subscription_count`, `device_count`, and a
  `business_line_mix` map whose counts sum to the tenant's subscriber total

#### Scenario: SCN-MCP-01.2 — Roamer split derived from usage
- **WHEN** the tool computes the roamer split
- **THEN** `roamers + non_roamers` equals the tenant's subscriber total, and a
  subscriber with no roaming usage event is counted as a non-roamer

### Requirement: REQ-MCP-02 — Usage-events analytics tool
The system SHALL provide `analyze_usage_events_tool(tenant_id, ...)` that reads
`fact_usage` and returns: (a) total event count, (b) **revenue per event type**
(sum of `gross_amount` grouped by `event_type`), (c) **duration/volume per event
type** (voice seconds from `qty` where `unit=seconds`, data bytes where
`unit=bytes`), and (d) **usage by event type over time** (counts/revenue bucketed
by day or month).

#### Scenario: SCN-MCP-02.1 — Revenue and duration by type
- **WHEN** the tool runs on a tenant with voice and data usage
- **THEN** `revenue_by_event_type` and `duration_by_event_type` each contain a
  `voice` and a `data` entry, and the event counts sum to the total

#### Scenario: SCN-MCP-02.2 — Time series present
- **WHEN** a time granularity of `day` or `month` is requested
- **THEN** the tool returns a time-bucketed series keyed by period and event type

### Requirement: REQ-MCP-03 — Recharge analytics tool
The system SHALL provide `analyze_recharges_tool(tenant_id, ...)` that reads
`fact_recharge` and returns: (a) **total top-up amount** (successful recharges),
(b) **recharge count**, (c) **average top-up value**, and (d) splits by
**recharge_type**, **payment_method**, and **time** (day or month).

#### Scenario: SCN-MCP-03.1 — Totals and average
- **WHEN** the tool runs on a tenant with recharges
- **THEN** `total_topup`, `recharge_count`, and `average_topup` are returned and
  `average_topup ≈ total_topup / successful_count`

#### Scenario: SCN-MCP-03.2 — Non-degenerate splits
- **WHEN** the recharge_type / payment_method splits are computed
- **THEN** each split is a map whose values sum to the corresponding recharge
  count (successful, for amount-based splits)

### Requirement: REQ-MCP-04 — Recharge-anomaly analytics tool
The system SHALL provide `analyze_recharge_anomalies_tool(tenant_id, ...)` that
reads `fact_recharge` and surfaces: (a) **recharge amount spikes** (recharges
whose amount exceeds a robust threshold, e.g. tenant median + k·MAD or a z-score),
(b) **recharge volume spikes** (subscriber-days with an unusually high recharge
count), (c) **subscriber-day anomaly detection** (the outlier `(service_id, day)`
pairs), and (d) **monthly/weekly aggregate views** of recharge amount and volume.

#### Scenario: SCN-MCP-04.1 — Amount and volume spikes flagged
- **WHEN** the tool runs with a configurable sensitivity
- **THEN** it returns an `amount_spikes` list and a `volume_spikes` list, each row
  identifying the tokenized `service_id`, the day, and the anomalous metric

#### Scenario: SCN-MCP-04.2 — Periodic views
- **WHEN** a `weekly` or `monthly` view is requested
- **THEN** the tool returns per-period total amount, recharge count, and a count
  of anomalous subscriber-days

### Requirement: REQ-MCP-05 — Fee-charge analytics tool
The system SHALL provide `analyze_fee_charges_tool(tenant_id, ...)` that reads
`fact_fee_charge` and returns: (a) **product fee revenue** (sum of charge amounts),
(b) **product revenue distribution** (revenue share per product), (c) **charge-type
analysis** (revenue and count per `charge_type`, e.g. recurring/one_time/overage/
discount), and (d) breakdowns by **product / category / month / quarter**.

#### Scenario: SCN-MCP-05.1 — Revenue distribution and charge types
- **WHEN** the tool runs on a tenant with fee charges
- **THEN** `total_fee_revenue`, a `revenue_by_product` distribution, and a
  `revenue_by_charge_type` map are returned, and the per-product shares sum to 1.0
  (± rounding)

#### Scenario: SCN-MCP-05.2 — Period breakdowns
- **WHEN** a `month` or `quarter` breakdown is requested
- **THEN** the tool returns fee revenue and charge count per period and per
  category

### Requirement: REQ-MCP-06 — Read-only, tenant-scoped MCP exposure
The five tools SHALL be exposed by an MCP server (`idp mcp`) as read-only tools.
Every tool SHALL require a `tenant_id` argument and read **only** that tenant's
Silver partition; no tool writes to the lake, and no tool returns raw
(untokenized) subscriber identifiers.

#### Scenario: SCN-MCP-06.1 — Tools discoverable over MCP
- **WHEN** an MCP client lists tools against the running server
- **THEN** all five `analyze_*_tool` tools are advertised with descriptions and
  input schemas

#### Scenario: SCN-MCP-06.2 — Tenant isolation enforced
- **WHEN** a tool is called with `tenant_id = tenant_a`
- **THEN** the result reflects only `tenant_a` rows, and identifiers in the output
  are the tokenized `service_id` values (no raw MSISDN/IMSI)

#### Scenario: SCN-MCP-06.3 — Missing tenant handled
- **WHEN** a tool is called with an unknown or missing `tenant_id`
- **THEN** the tool returns a structured empty/error result rather than raising, and
  never leaks another tenant's data
