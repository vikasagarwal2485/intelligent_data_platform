# Tasks — Phase 0: Foundation

## 1. Packaging & tooling
- [ ] 1.1 Create `pyproject.toml` (package `idp`, `src/` layout, console script `idp`)
- [ ] 1.2 Add `requirements.txt` / `requirements-dev.txt`
- [ ] 1.3 Add `Makefile` targets: `install`, `test`
- [ ] 1.4 Add `.github/workflows/ci.yml` running `pip install -e .[dev]` + `pytest`

## 2. Configuration
- [ ] 2.1 Write `config/platform.yaml` (tenants w/ per-tenant `pii_salt`, storage,
      kafka, s3 `bucket_prefix`, synthetic, features.windows, credit, fraud)
- [ ] 2.2 Implement `common/config.py` with typed accessors and `IDP_*` overrides
      (`IDP_S3_ENDPOINT` enables S3; `IDP_SUBSCRIBERS`/`IDP_DAYS`/`IDP_FRAUD_RATE`)

## 3. Multi-tenancy & GDPR primitives
- [ ] 3.1 `common/tenancy.py`: `stamp_tenant`, `scope_to_tenant`,
      `require_tenant_column`, `assert_single_tenant`, `TenantIsolationError`
- [ ] 3.2 `common/pii.py`: `tokenize_value/series/columns` (per-tenant salted
      sha256), `filter_by_consent`
- [ ] 3.3 `common/storage.py`: `storage_options`, `lake_uri` (`<prefix>-<layer>`),
      `write_parquet`, `read_parquet`

## 4. Synthetic data
- [ ] 4.1 `synthetic/generator.py`: coherent 7-entity generation, deterministic
      seed (hashlib, not builtin `hash`), per-tenant, `tenant_id` stamped
- [ ] 4.2 Emit labels (`is_fraud`, `fraud_type`, `will_default`,
      `true_credit_limit`) with configurable rates + injected behaviour
- [ ] 4.3 `generate` (single tenant) and `generate_for_tenants`

## 5. Contracts & tests
- [ ] 5.1 Add `config/avro/{usage_event,recharge_event,subscriber}.avsc`
      (namespace `com.idp`)
- [ ] 5.2 Tests: config overrides, tenancy guards, pii determinism/consent,
      generator integrity + determinism + label signal
- [ ] 5.3 `make test` green

## Done when
`specs/foundation` and `specs/synthetic-data` scenarios pass and CI is green.
