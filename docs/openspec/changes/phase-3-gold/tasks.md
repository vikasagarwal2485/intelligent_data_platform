# Tasks — Phase 3: Gold Features

## 1. Aggregation backbone
- [ ] 1.1 `features/base.py`: `recharge_aggregates`, `usage_aggregates`,
      `merge_features` with configurable lookback windows (7/30/90d), keyed by
      `(tenant_id, service_id)`

## 2. Feature sets
- [ ] 2.1 `credit_features.py`: recharge reliability/ability, spend, ARPU,
      `spend_to_recharge_ratio`, tenure, products (list `CREDIT_FEATURES`)
- [ ] 2.2 `fraud_features.py`: `calls_per_active_day`, `originating_ratio`,
      `unique_called_numbers`, `roaming_share`, `failed_recharge_count`,
      per-tenant `recharge_amount_zscore` (list `FRAUD_FEATURES`)
- [ ] 2.3 No nulls in model feature columns; ratios within [0,1]

## 3. Stage function
- [ ] 3.1 `pipeline.to_gold(cfg, silver=None, persist=True, models=None)`: read
      Silver if `None`; build features; append scores only if models available;
      write Gold partitioned by `tenant_id`

## 4. Tests
- [ ] 4.1 Feature shape/columns/bounds; join coverage; features-only vs with-models
- [ ] 4.2 `make test` green

## Done when
`specs/gold-features` scenarios pass.
