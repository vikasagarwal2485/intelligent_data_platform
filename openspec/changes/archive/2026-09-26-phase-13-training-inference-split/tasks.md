# Phase 13 — Tasks

## Labels store + lake-based training (REQ-ML-06)
- [x] `storage.labels_uri` (s3://<prefix>-ml/labels or local)
- [x] `pipeline.to_labels` (tokenized) + `_read_labels`; `idp labels`; `make labels`; labels-job.yaml
- [x] `train` lake path: read Gold features + labels store, join, fit (no synthetic regen); clear error if missing
- [x] `train` in-memory path kept for local run/tests
- [x] Tests: labels roundtrip (tokenized); train-from-lake fits + persists; lake-path errors without labels

## Gold features-only + separate Score stage (REQ-ML-07)
- [x] `to_gold` features-only (drop scoring)
- [x] `to_scores` (Gold features + models from store -> score tables); `idp score`; `make score`; score-job.yaml
- [x] `run` recomposed: bronze -> silver -> labels -> gold -> train -> score
- [x] Tests: score stage from provided models; score loads models from store; gold has no scores

## Two DAGs (REQ-ORC-02)
- [x] Refresh DAG: silver >> gold >> score >> publish
- [x] Train DAG: labels >> train
- [x] gold-job comment features-only

## Docs / spec
- [x] `specs/ml-models`: REQ-ML-06, REQ-ML-07
- [x] `specs/orchestration`: two-DAG REQ-ORC-02
- [x] `decisions/decision-log.md`: ADR-0025
- [x] `roadmap.md` row 13; regenerate traceability
- [x] README bring-up ordering + ML section
- [x] `make test` green
