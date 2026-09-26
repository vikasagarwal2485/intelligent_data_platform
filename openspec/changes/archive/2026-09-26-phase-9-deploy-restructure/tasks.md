# Tasks — Phase 9: Deploy Directory Restructure

## 1. Platform subtree (`deploy/platform/`)
- [x] 1.1 Move `airflow/`, `argocd/`, `overlays/` and `base/` under `platform/`
- [x] 1.2 `infra/keycloak.yaml` → `platform/keycloak/keycloak.yaml`;
      `infra/timescaledb.yaml` → `platform/timescaledb/timescaledb.yaml`; remove `infra/`
- [x] 1.3 `trino-values.yaml` → `platform/trino/`; `nessie-values.yaml` → `platform/nessie/`
- [x] 1.4 Move `kafka/` under `platform/` (keeps cluster, schema-registry,
      topics/schema scripts, kafka-ui)
- [x] 1.5 Move root `*.sh` (`install-infra.sh`, `strimzi-tune.sh`, `trino-tune.sh`,
      `nessie-tune.sh`) into `platform/`

## 2. App subtree (`deploy/app/`)
- [x] 2.1 Move `jobs/` (silver/gold/publish/spark-gold) → `app/jobs/`
- [x] 2.2 Move `kafka/{produce,consume}-job.yaml` → `app/jobs/`
- [x] 2.3 Move `base/scoring-api.yaml` → `app/deployment/scoring-api.yaml`;
      add `app/deployment/kustomization.yaml`

## 3. Kustomize rewiring
- [x] 3.1 `platform/base/kustomization.yaml`: drop `scoring-api.yaml`
- [x] 3.2 `platform/overlays/{local,aws}`: resources `../../base` + `../../../app/deployment`;
      keep the scoring-api patch

## 4. Reference updates (no broken paths)
- [x] 4.1 `Makefile`: all script/manifest/overlay paths
- [x] 4.2 `platform/install-infra.sh` + `{trino,nessie}-tune.sh`: relative paths
- [x] 4.3 `platform/argocd/application.yaml`: `path: deploy/platform/overlays/aws`
- [x] 4.4 Comments/docs: `config/platform.yaml`, `storage.py`, DAG, `triggering.md`,
      plan tree, OpenSpec phase-8 proposal/tasks, `project.md`
- [x] 4.5 Rewrite `deploy/README.md` and root README deploy sections

## 5. Verify
- [x] 5.1 `git mv` preserves history; no file left in the old locations
- [x] 5.2 `kustomize build` succeeds for both overlays
- [x] 5.3 `bash -n` clean for every moved script; `pytest` green; no stale `deploy/` refs

## Done when
`deploy/platform/**` and `deploy/app/**` are the only two subtrees (plus
`deploy/README.md`), both overlays build, and no reference points at an old path.
