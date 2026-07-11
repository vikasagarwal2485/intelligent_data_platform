# Tasks — Phase 1: Bronze Ingestion

## 1. Bronze persistence
- [ ] 1.1 `ingestion/bronze.py`: write frames to `bronze/<entity>/` partitioned by
      `tenant_id`; reload restoring `tenant_id`
- [ ] 1.2 Define `ENTITIES` (parties, accounts, agreements, products, voice_events,
      data_events, recharge_events) in `pipeline.py`
- [ ] 1.3 `pipeline.to_bronze(cfg, frames, persist=True)` using `common/storage`
      so it targets local dirs or `s3://<prefix>-bronze` per config

## 2. Guarantees
- [ ] 2.1 Reject/ignore frames without `tenant_id`; never transform in Bronze
- [ ] 2.2 Ensure `to_bronze` performs no Silver/Gold work

## 3. Tests
- [ ] 3.1 Bronze write→reload round-trip; partition dirs per tenant
- [ ] 3.2 `make test` green

## Done when
`specs/bronze-ingestion` scenarios pass.
