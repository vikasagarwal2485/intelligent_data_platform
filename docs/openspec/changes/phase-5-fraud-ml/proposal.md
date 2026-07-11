# Phase 5 — Fraud-Detection ML

## Why
Deliver the fraud product: a layered detector combining rules, a supervised
classifier, and anomaly detection.

## What Changes
- Implement `ml/fraud_model.py`: `rule_flags` (SIM-box/recharge/roaming),
  `FraudModel` (gradient-boosted classifier with balanced weights + IsolationForest;
  fused `score`; `evaluate` with PR-AUC/precision@k; save/load).
- Wire the fraud half of `pipeline.train`.

## Impact
- Spec deltas: `ml-models` (ADDED fraud requirements).
- Code: `src/idp/ml/fraud_model.py`, `src/idp/pipeline.py` (`train` fraud branch).
- Decisions: ADR-0015 (layered fusion), ADR-0004 (rules portable for later Flink).

## Spec Deltas
Satisfies the fraud requirements in `specs/ml-models`.
