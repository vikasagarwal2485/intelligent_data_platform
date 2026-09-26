# Phase 13 — Train on historical Gold + labels store; split Gold/Score into two DAGs

Date: 2026-07-20

## Why
Phase 12 gave models a shared store but left two things off: training still
**regenerated synthetic data in memory** (it didn't learn from the lake), and
**scoring was folded into Gold**, which forced the unintuitive "train before gold"
order and coupled feature-build with inference. Real systems train a model
periodically on **historical features + observed outcomes**, and score the latest
features on a separate cadence.

## What Changes
- **Labels/outcomes store** — `common/storage.labels_uri` → `s3://<prefix>-ml/labels`
  (local dir otherwise), kept **outside** the Gold/serving path so labels never
  leak into features or the published tables. `pipeline.to_labels` writes the
  tokenized labels; `idp labels` / `make labels` / `labels-job.yaml` seed it —
  `REQ-ML-06`.
- **Training reads the lake** — `pipeline.train`, when called with no in-memory
  dataset (the `idp train` job), reads **Gold features + the labels store** from
  storage, joins, fits, and saves to the model store — **no synthetic
  regeneration**. It errors clearly if Gold features or labels are missing. The
  in-memory path stays for local `run`/tests — `REQ-ML-06`.
- **Gold is features-only; Score is a separate stage** — `to_gold` no longer
  scores; new `to_scores` (`idp score` / `make score` / `score-job.yaml`) reads
  Gold features + models from the store and writes `credit_scores`/`fraud_scores`
  back to Gold. Graceful when a model is absent — `REQ-ML-07`.
- **Two DAGs** (`pipelines/airflow/dags/idp_medallion_dag.py`):
  1. **Refresh** (`idp_silver_gold`, hourly): `silver >> gold >> score >> publish`.
  2. **Training** (`idp_train`, daily): `labels >> train`.
  — `REQ-ORC-02`.
- README/Makefile bring-up updated to the new stage set and ordering.

## Impact
- Spec deltas: `ml-models` — ADD `REQ-ML-06` (labels store + lake training),
  `REQ-ML-07` (separate score stage); `orchestration` — UPDATE `REQ-ORC-02` (two
  DAGs incl. score + labels).
- Code: `common/storage.py`, `pipeline.py` (to_labels/_read_labels/to_scores; gold
  features-only; train lake path; run recomposed), `cli.py` (labels/score),
  `deploy/app/jobs/{score,labels}-job.yaml`, `gold-job.yaml`, `Makefile`,
  `pipelines/airflow/dags/idp_medallion_dag.py`, `tests/**`.
- Decisions: ADR-0025 (labels/outcomes store + lake-based training + score/gold
  split; refines ADR-0014).

## Bring-up order (local)
`… make stream → make silver → make labels → make gold → make train → make score →
make publish`. Or the two DAGs: refresh runs silver→gold→score→publish; the daily
train DAG runs labels→train.

## Spec Deltas
Satisfies `REQ-ML-06`, `REQ-ML-07`, and the two-DAG `REQ-ORC-02` in
`specs/ml-models` and `specs/orchestration`.
