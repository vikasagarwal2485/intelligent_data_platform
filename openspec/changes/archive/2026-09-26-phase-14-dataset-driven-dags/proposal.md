# Phase 14 — Dataset-driven orchestration: three DAGs (features / inference / training)

Date: 2026-07-20

## Why
Phase 13's refresh DAG (`silver >> gold >> score >> publish`) coupled scoring to
the feature cadence. Scoring needs a model produced by the training DAG on a
different cadence, so: cold start wasted a score run (no model yet), and a newly
trained model only reached serving after a **manual re-trigger**. Feature-build,
inference and training are three concerns on three cadences and should be three
pipelines.

## What Changes
- **Split into three Airflow DAGs** (`pipelines/airflow/dags/idp_medallion_dag.py`):
  - `idp_features`  — `silver >> gold`  (hourly; **produces** the gold-features dataset)
  - `idp_inference` — `score  >> publish`  (**dataset-driven**)
  - `idp_training`  — `labels >> train`  (daily; **produces** the model dataset)
  — `REQ-ORC-02`.
- **Data-aware scheduling (Airflow Datasets)** — the `gold` task `outlets`
  `GOLD_FEATURES`; the `train` task `outlets` `MODEL`; `idp_inference` is scheduled
  on `(GOLD_FEATURES | MODEL)` (OR, Airflow >= 2.9), so it re-runs on a feature
  refresh OR a new model. Inference loads the current model each run → a promoted
  model **auto-re-scores** with no manual pass — `REQ-ORC-02`.
- **`make airflow-trigger`** unpauses all three DAGs and kicks features + training
  (inference fires on the datasets); README/spec updated.

## Impact
- Spec deltas: `orchestration` — UPDATE `REQ-ORC-02` (three DAGs, dataset-driven).
- Code: `pipelines/airflow/dags/idp_medallion_dag.py`, `Makefile`, `README.md`.
- Decisions: ADR-0026 (three dataset-driven DAGs; refines ADR-0025).

## Spec Deltas
Satisfies the three-DAG, dataset-driven `REQ-ORC-02` in `specs/orchestration`.
