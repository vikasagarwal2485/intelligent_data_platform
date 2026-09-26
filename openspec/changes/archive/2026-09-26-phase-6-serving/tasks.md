# Tasks — Phase 6: Serving API

## 1. API
- [x] 1.1 `serving/api.py`: `create_app(artifacts_dir)`; `GET /health`
- [x] 1.2 `POST /credit-limit` → `{pd, affordability_base, recommended_limit}`
- [x] 1.3 `POST /fraud-score` → `{fraud_proba, anomaly_score, rule_score, fraud_score}`
- [x] 1.4 Lazy-load models; return HTTP 503 when absent; echo/scope `tenant_id`

## 2. Gold serving materialization
- [x] 2.1 `pipeline.publish_serving(cfg)`: read Gold, upsert into TimescaleDB
      `gold.*` indexed by `(tenant_id, service_id)`; expose as `idp publish`

## 3. Packaging
- [x] 3.1 `idp serve` CLI (uvicorn)
- [x] 3.2 `Dockerfile` building the `idp` package and running `idp serve` (non-root)

## 4. Tests
- [x] 4.1 health; endpoints return expected keys after training; 503 without models
- [x] 4.2 `make test` green

## Done when
`specs/serving-api` scenarios pass.
