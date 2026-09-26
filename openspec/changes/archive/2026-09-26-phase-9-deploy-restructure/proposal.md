# Phase 9 — Deploy Directory Restructure (team separation of concerns)

## Why
`deploy/` had grown into a flat mix of platform infrastructure and application
workloads (Airflow, Kafka, Trino, Nessie, Keycloak, TimescaleDB, base/overlays,
scoring-api, stage jobs, tuning scripts all at one level). Different engineering
teams own these: a **platform team** owns the shared services; an **application
team** owns the workloads that run on top. A flat layout makes ownership,
review, and RBAC boundaries unclear. This change reorganizes `deploy/` into
`platform/` and `app/` subtrees so each team can maintain and deploy its area
independently. No manifest content or runtime behavior changes — only locations
and the references to them.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- `deploy/platform/**` — platform-team infrastructure: `base/` (namespaces, RBAC,
  isolation), `overlays/{local,aws}`, `argocd/`, `kafka/`, and per-service dirs
  `keycloak/`, `timescaledb/`, `trino/`, `nessie/`, `airflow/`, plus
  `install-infra.sh` and the `*-tune.sh` repair scripts — `REQ-PLT-01`,
  `REQ-PLT-02`, `REQ-PLT-03`, `REQ-PLT-04`.
- `deploy/app/**` — application-team workloads: `deployment/scoring-api.yaml`
  and `jobs/{produce,consume,silver,gold,publish,spark-gold}-job.yaml` —
  `REQ-PLT-01`, `REQ-ORC-01`, `REQ-ORC-02`.
- Kustomize rewiring: `platform/base` no longer contains scoring-api; a new
  `app/deployment/kustomization.yaml` owns it; both `platform/overlays/{local,aws}`
  now compose `../../base` + `../../../app/deployment` and keep the scoring-api
  image/replica/pull-policy patch — `REQ-PLT-01`, `REQ-PLT-04`.
- Reference updates so nothing breaks: `Makefile` targets, `install-infra.sh`
  and `{trino,nessie}-tune.sh` relative paths, `argocd/application.yaml` sync
  path, `config/platform.yaml` + `storage.py` comments, `docs/triggering.md`,
  the plan's repo-layout tree, `deploy/README.md`, and the root README — all
  supporting the requirements above (no new behavior).

## Impact
- Spec deltas: none — this is a structural refactor; all `platform-deployment`
  and `orchestration` requirements are unchanged and still satisfied.
- Code: `deploy/**` (moves via `git mv`), `Makefile`, `config/platform.yaml`,
  `src/idp/common/storage.py` (comment), `pipelines/airflow/dags/idp_medallion_dag.py`
  (comment), `docs/**`, `README.md`, `deploy/README.md`.
- Decisions: ADR-0018 (deploy layout: platform vs app separation of concerns).
- Operational: no re-install required. Helm releases are unaffected (same charts,
  same values content). Only the local file paths passed to `helm`/`kubectl`/
  `make` changed, so existing clusters keep running; images are unchanged.

## Spec Deltas
None. Re-verifies the existing `specs/platform-deployment` and
`specs/orchestration` scenarios against the relocated files.
