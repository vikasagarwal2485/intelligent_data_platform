# Tasks — Phase 6: Serving API

## 1. API
- [ ] 1.1 `serving/api.py`: `create_app(artifacts_dir)`; `GET /health`
- [ ] 1.2 `POST /credit-limit` → `{pd, affordability_base, recommended_limit}`
- [ ] 1.3 `POST /fraud-score` → `{fraud_proba, anomaly_score, rule_score, fraud_score}`
- [ ] 1.4 Lazy-load models; return HTTP 503 when absent; echo/scope `tenant_id`

## 2. Gold serving materialization
- [ ] 2.1 `pipeline.publish_serving(cfg)`: read Gold, upsert into TimescaleDB
      `gold.*` indexed by `(tenant_id, service_id)`; expose as `idp publish`

## 3. Packaging
- [ ] 3.1 `idp serve` CLI (uvicorn)
- [ ] 3.2 `Dockerfile` building the `idp` package and running `idp serve` (non-root)

## 4. Tests
- [ ] 4.1 health; endpoints return expected keys after training; 503 without models
- [ ] 4.2 `make test` green

## Done when
`specs/serving-api` scenarios pass.
