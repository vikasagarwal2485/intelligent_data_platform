# Tasks — Phase 5: Fraud-Detection ML

## 1. Rules & model
- [ ] 1.1 `rule_flags(features, thresholds)`: SIM-box (originating ratio + velocity),
      recharge-burst, roaming; normalized `rule_score`
- [ ] 1.2 `FraudModel.fit`: `HistGradientBoostingClassifier` with balanced sample
      weights + `IsolationForest`
- [ ] 1.3 `score`: fuse `0.6*proba + 0.2*anomaly + 0.2*rules` ∈[0,1]
- [ ] 1.4 `evaluate`: PR-AUC, precision@k; `save`/`load`

## 2. Training wiring
- [ ] 2.1 Fraud branch of `pipeline.train`: features + labels, imbalance-aware
      out-of-sample fit, persist model

## 3. Tests
- [ ] 3.1 Rule flags; scores∈[0,1]; fraud ranked > normal; PR-AUC > base rate;
      save/load parity
- [ ] 3.2 `make test` green

## Done when
The fraud scenarios in `specs/ml-models` pass.
