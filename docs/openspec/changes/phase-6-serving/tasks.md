# Tasks — Phase 6: Serving API

## 1. API
- [ ] 1.1 `serving/api.py`: `create_app(artifacts_dir)`; `GET /health`
- [ ] 1.2 `POST /credit-limit` → `{pd, affordability_base, recommended_limit}`
- [ ] 1.3 `POST /fraud-score` → `{fraud_proba, anomaly_score, rule_score, fraud_score}`
- [ ] 1.4 Lazy-load models; return HTTP 503 when absent; echo/scope `tenant_id`

## 2. Packaging
- [ ] 2.1 `idp serve` CLI (uvicorn)
- [ ] 2.2 `Dockerfile` building the `idp` package and running `idp serve` (non-root)

## 3. Tests
- [ ] 3.1 health; endpoints return expected keys after training; 503 without models
- [ ] 3.2 `make test` green

## Done when
`specs/serving-api` scenarios pass.
