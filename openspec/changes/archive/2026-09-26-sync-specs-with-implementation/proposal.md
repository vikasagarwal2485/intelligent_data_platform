# Proposal

## Why

The main specs under `openspec/specs/` drifted from the code across phases 8–15:
one requirement contradicts another (REQ-GLD-03 vs REQ-ML-07), several describe
superseded behaviour (train reading Silver, publish "upserting"), and shipped
behaviour (serving lookup endpoint, score history, MCP tenant guard, labels-store
rules, landing zone, Kafka wire format) has no requirement at all. The specs must
be a trustworthy baseline before the gap-fixing changes (`idempotent-stage-writes`,
`spark-engine-parity`, `consent-and-tenant-gateway`, `mlflow-model-registry`,
`oidc-tenant-auth`) layer their deltas on top.

This change records **already-implemented** behaviour only. Where the code falls
short of the intended behaviour, the existing requirement is left as-is and the gap
is owned by one of the changes above.

## What Changes

- **REMOVED** REQ-GLD-03 (Gold optional scoring) — superseded by REQ-ML-07; Gold is
  features-only.
- **MODIFIED** REQ-ORC-01 — stage list gains `produce`, `labels`, `score` and the
  Spark stage jobs.
- **MODIFIED** REQ-ORC-02 — Spark implements Silver as well as Gold, each stage has an
  independent engine switch, and DAG cadences are stated.
- **MODIFIED** REQ-ML-03 — the `train` job fits on Gold features + the labels store
  (not Silver); in-memory path is dev/test only. The MLflow wording moves to
  `mlflow-model-registry`.
- **MODIFIED** REQ-ML-06 — labels store keeps the latest label per subscriber, must
  be generated with the same synthetic parameters as the data, and an empty join is
  a clear error.
- **MODIFIED** REQ-ML-07 — each score run stamps `scored_at` and accumulates history.
- **MODIFIED** REQ-SRV-01 — publish replaces each serving table atomically and is
  safe under concurrent publishers (not an "upsert").
- **MODIFIED** REQ-SRV-02 — `/credit-limit` returns adverse-action `reason_codes`.
- **ADDED** REQ-SRV-04 — precomputed serving lookup endpoint.
- **MODIFIED** REQ-SLV-01 (four canonical tables) and REQ-SLV-05 (`agreement_id`
  carried on the fee fact).
- **MODIFIED** REQ-FND-01 — `timescale` config section and the full env-override set.
- **MODIFIED** REQ-MCP-02 (week/quarter granularity) and REQ-MCP-06 (tenant guard:
  `available_tenants`/`action_required`, unreachable-lake error).
- **ADDED** REQ-BRZ-03 — landing zone written by `idp generate`.
- **MODIFIED** REQ-STR-01 (full topic set incl. `fees`) and REQ-STR-03 (JSON message
  encoding; Avro schemas are the registered contract).
- **MODIFIED** REQ-PLT-02 — `idp-ml` bucket, Airflow installed separately, Kafka UI.
- Housekeeping: fix `docs/gen_traceability.py` paths after the `docs/openspec` →
  `openspec` move, correct stale "5 tools" help text, and archive the 16 completed
  phase changes.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `bronze-ingestion`: add the landing-zone requirement.
- `foundation`: config surface and env overrides.
- `gold-features`: remove optional scoring.
- `mcp-analytics`: granularity values and the tenant/lake guard.
- `ml-models`: train inputs, labels-store rules, score history.
- `orchestration`: full stage list, Spark Silver, engine switch, cadences.
- `platform-deployment`: buckets and install topology.
- `serving-api`: publish semantics, reason codes, lookup endpoint.
- `silver-curation`: four tables; fee fact carries `agreement_id`.
- `streaming-kafka`: topic set and message encoding.

## Impact

- Specs only; no runtime behaviour changes.
- Tooling: `docs/gen_traceability.py`, `docs/traceability.md` regenerated.
- Cosmetic code text: `src/idp/cli.py` and `Makefile` help strings,
  `src/idp/pipeline.py` module docstring.
- Must be archived **before** the five gap-fixing changes so their deltas apply to
  the corrected baseline.
