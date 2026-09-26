# Phase 14 — Tasks

## Three dataset-driven DAGs (REQ-ORC-02)
- [x] `idp_features` (silver >> gold); gold task `outlets` GOLD_FEATURES
- [x] `idp_training` (labels >> train); train task `outlets` MODEL
- [x] `idp_inference` (score >> publish) scheduled on `(GOLD_FEATURES | MODEL)`
- [x] Dataset definitions (logical s3 URIs) + `from airflow.datasets import Dataset`
- [x] `make airflow-trigger` unpauses all three + triggers features & training
- [x] DAG module docstring updated (three DAGs, dataset-driven)

## Docs / spec
- [x] `specs/orchestration`: REQ-ORC-02 -> three DAGs, dataset-driven
- [x] `decisions/decision-log.md`: ADR-0026
- [x] `roadmap.md` row 14; regenerate traceability
- [x] README (bring-up note + §3.2/§4.4 DAG references)
