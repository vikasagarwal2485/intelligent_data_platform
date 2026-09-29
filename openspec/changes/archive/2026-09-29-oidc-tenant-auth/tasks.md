# Tasks

## 1. Configuration and dependency

- [x] 1.1 Add an `auth` config section (`enabled` false, `issuer`, `audience`, `tenant_claim`, `leeway_seconds`) with `IDP_AUTH_*` env overrides. Verify with a config test
- [x] 1.2 Add `PyJWT[crypto]` to `requirements.txt`. Verify that `import jwt` works and `PyJWKClient` is available

## 2. Token validation

- [x] 2.1 Create `src/idp/serving/auth.py` with the `current_tenant()` dependency: Bearer parsing, JWKS verification of signature/iss/aud/exp, and tenant-claim extraction (401/403 as specified, 503 when the JWKS cannot be fetched). Verify with unit tests using a generated RSA keypair and mocked JWKS
- [x] 2.2 Wire the dependency into `/credit-limit`, `/fraud-score` and `/serving/...`, overriding or rejecting a mismatching body/path `tenant_id`; `/health` stays open. Verify with API tests for SCN-SRV-03.3, 03.4 and 03.5
- [x] 2.3 Keep dev mode: with `auth.enabled=false`, the existing API tests pass unchanged. Verify SCN-SRV-03.1 and 03.2 with a valid token when auth is enabled

## 3. Keycloak realm

- [x] 3.1 Add `deploy/platform/keycloak/idp-realm.json` (realm `idp`, client `idp-scoring-api` with audience mapper, `tenant_id` mapper, dev users per tenant, per-tenant client-credentials clients) as a ConfigMap, mount it, and switch args to `start-dev --import-realm` with `KC_HOSTNAME` set. Verify that after a pod restart the realm exists and a token contains `tenant_id`
- [x] 3.2 Add a `make keycloak-token TENANT=tenant_a` target that prints an access token via client credentials. Verify that the decoded token has the correct `iss`, `aud` and `tenant_id`

## 4. Deployment and docs

- [x] 4.1 Set `IDP_AUTH_ENABLED=true`, `IDP_AUTH_ISSUER` and `IDP_AUTH_AUDIENCE` in both app overlays (local/aws). Verify on minikube that an unauthenticated call gets 401 and a Bearer call gets 200 (SCN-SRV-03.6)
- [x] 4.2 Document the token flow and the breaking change in `deploy/README.md`. Verify the doc covers obtaining a token and calling each endpoint
