# Tasks — Phase 2: Silver Curation

## 1. JSON:API unwrap
- [x] 1.1 `curation/jsonapi.py`: `strip_uuid_wrapper`, `normalize_money`
      (net+tax→gross), `unwrap_usage_event`, `unwrap_recharge_event`
- [x] 1.2 Verify against real `data_samples/*.json`

## 2. Canonical tables
- [x] 2.1 `build_subscriber_dim`: join party/account/agreement + product counts,
      tenure days, one row per `(tenant_id, service_id)`, dedup
- [x] 2.2 `build_fact_usage`: union voice+data, `gross_amount = net+tax`, UTC ts, dedup
- [x] 2.3 `build_fact_recharge`: normalize ts/amount, dedup
- [x] 2.4 Tokenize PII per tenant in all three (consistent tokens for joins)
- [x] 2.5 `curate(bronze, salts)` returns the three tables

## 3. Stage function
- [x] 3.1 `pipeline.to_silver(cfg, bronze=None, persist=True)`: read Bronze if
      `None`, guard required entities, write Silver partitioned by `tenant_id`

## 4. Tests
- [x] 4.1 Unwrap vs `data_samples`; money/UUID; token join integrity; grain; isolation
- [x] 4.2 `make test` green

## Done when
`specs/silver-curation` scenarios pass.
