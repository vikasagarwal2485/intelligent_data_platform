# Tasks — Phase 4: Credit-Limit ML

## 1. Model
- [ ] 1.1 `heuristic_limit(features, ...)`: affordability cold-start, clipped to bands
- [ ] 1.2 `CreditLimitModel.fit`: `HistGradientBoostingClassifier` PD model +
      logistic scorecard baseline
- [ ] 1.3 `predict_pd` (∈[0,1]); `recommend_limit` = affordability × (1−PD), clipped
- [ ] 1.4 `evaluate`: AUC, KS (`ks_statistic`), Gini; `save`/`load`

## 2. Training wiring
- [ ] 2.1 `pipeline._safe_split` (stratified holdout, degrades on tiny data)
- [ ] 2.2 `pipeline._tokenize_labels` (join labels to tokenized `service_id`)
- [ ] 2.3 Credit branch of `pipeline.train`: build features, merge labels, fit,
      evaluate out-of-sample, persist model

## 3. Tests
- [ ] 3.1 PD∈[0,1]; higher PD → not-higher limit; limits within band; AUC>0.5;
      save/load parity
- [ ] 3.2 `make test` green

## Done when
The credit-limit + training scenarios in `specs/ml-models` pass.
