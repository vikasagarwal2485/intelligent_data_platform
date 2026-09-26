# Proposal

## Why

Two foundation guarantees exist as helper functions but are never enforced where
data is used.

- **Consent (REQ-FND-03):** `pii.filter_by_consent` is tested, but no stage calls it.
  About 10% of synthetic subscribers have `consent_analytics = false`, and they are
  still featurized, trained on, scored and published. The `pii.py` docstring claims
  the opposite. This is a GDPR gap.
- **Tenant gateway (REQ-FND-02):** `scope_to_tenant` is meant to be "the single
  gateway" for tenant-scoped reads. The MCP tools filter by `tenant_id` with their
  own code, and a table that lacks `tenant_id` silently reads as "empty" instead of
  failing as an isolation violation.

## What Changes

- Subscribers without analytics consent are excluded when Gold features are built,
  once, in the Spark feature transforms (the single engine), so they never reach
  training, scoring, the serving store or score analytics. Missing or null consent counts as **no consent** (fail closed).
- A lake table used for model features that has no consent information fails the
  Gold stage loudly instead of passing everyone through.
- Every tenant-scoped read in the MCP analytics path goes through the tenant
  gateway. Untagged data surfaces as a structured tenant-isolation error, never as
  empty or unscoped data.
- Consent revocation takes effect on the next Gold refresh plus Score/publish.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `foundation`: add REQ-FND-05 (consent enforced on the ML/serving path) and
  REQ-FND-06 (tenant-scoped reads through the gateway).

## Impact

- Code:
  - `src/idp/transforms/features.py` (Spark consent filter on `dim_subscriber`)
  - `src/idp/common/pii.py` (`ConsentError`; docstring fixed to name the real
    enforcement point)
  - `src/idp/mcp/tools.py` (read via `scope_to_tenant`)
- Data: Gold, scores and serving shrink by the non-consenting share. Tests asserting
  "one feature row per subscriber" change to "per consenting subscriber". The golden
  Gold files (`tests/golden/gold/`) are regenerated; the only difference must be the
  non-consenting rows.
- Ordering: after `idempotent-stage-writes` (so a revoked subscriber's old Gold rows
  are removed on refresh) and `spark-single-engine` (so the filter is written once,
  in `idp.transforms`). Both are archived.
- Non-goal: aggregate Silver analytics (MCP tools 1–5) keep counting all
  subscribers; see design.md.
