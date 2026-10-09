# Tasks

## 1. Prove the Trino mechanics first (compose test stack)

- [x] 1.1 Spike on the compose Trino 480 stack with a hand-written JWKS and rules:
  - JWT authentication with `principal-field=idp_principal`;
  - `current_user` inside the row filter resolves to the querying user, and to the
    impersonated user under `superset` impersonation;
  - `$partitions`/`$files` are denied by the `.*\$.*` rule;
  - `SHOW STATS` as a tenant principal doesn't return table-wide statistics.

  Record the results in design.md; if any check fails, stop and revise the design
  (Risks) before continuing.
- [x] 1.2 Add `deploy/platform/trino/make_secrets.py` (Python, `cryptography` +
  `bcrypt`, so it runs on CI runners without `htpasswd`). It writes a self-signed
  certificate, a bcrypt password file for `admin`/`superset` with their passwords,
  the shared secret and, with `--test-jwt`, a test key pair, to a directory, keeping
  existing files unless `--rotate`. `make trino-stack` runs it into the git-ignored
  `deploy/dev/trino/.secrets` before `docker compose up`, and the stack serves HTTPS
  with JWT and password authentication. Verify the stack comes up healthy, `admin`
  authenticates with its password, a header-only request gets 401, and a test token
  authenticates.

## 2. Keycloak identities rendered from the tenants

- [x] 2.1 Split `idp-realm.json` into a base file (realm settings, a declarative user
  profile with `tenant_id`/`idp_principal` admin-editable only, the
  `idp-scoring-api` and `idp-superset` clients, `idp-dev-cli` with an `idp_principal`
  mapper, and the dev users with both attributes and their tenant group). Verify the
  base parses and the existing auth tests still pass (`pytest tests/test_auth.py`).
- [x] 2.2 Add `idp keycloak-realm` (`src/idp/identity/realm.py`). Per configured tenant
  it renders a group, the existing `idp-tenant-<t>` scoring client, and an
  `idp-mcp-<t>` client (client credentials; hard-coded `tenant_id` and `idp_principal
  = mcp@<t>` mappers; audience `trino`). Secrets come from `IDP_KC_SECRET_<TENANT>`,
  else the documented dev defaults. Commit the rendered realm, and verify with a test
  that it equals the committed file (SCN-TEN-02.1) and that adding `tenant_c` adds
  only its group and clients (SCN-TEN-02.2, render level).
- [x] 2.3 Extend `deploy/platform/keycloak/token.sh` and `make keycloak-token` with
  `KIND=scoring|mcp|user` (`KEYCLOAK_URL` targets an already reachable Keycloak).
  Verified against the rendered realm imported into a local Keycloak 26.0 container
  (the cluster's image; the minikube check is part of task 7.2):
  - a `tenant_b` MCP token carries `idp_principal = mcp@tenant_b`,
    `tenant_id = tenant_b` and audience `trino` (SCN-TEN-01.2);
  - Alice's token carries `alice@tenant_a` / `tenant_a` and audience `trino`
    (SCN-TEN-01.1);
  - with self-service enabled, changing her own `tenant_id`/`idp_principal` through
    the account API is refused as read-only and her next token is unchanged
    (SCN-TEN-01.3).

## 3. Trino authentication, HTTPS and row-level security

- [x] 3.1 Add `make trino-certs`, which creates the `trino-tls` (certificate and key),
  `trino-auth` (bcrypt `password.db` for `admin` and `superset`) and
  `trino-internal` (shared secret) Secrets in `idp-platform` and writes the CA
  certificate to a known host path. It's idempotent: re-running keeps existing
  secrets unless `ROTATE=1`. Verify with `kubectl get secret`.
- [x] 3.2 Update `deploy/platform/trino/trino-values.yaml`:
  - JWT and password authentication, HTTPS on 8443, the shared secret;
  - Secret mounts;
  - probes on HTTP 8080;
  - a `trino` service port 8443.

  Verify on minikube with `make trino-fix`: the pod is ready, and an `X-Trino-User`
  request without a credential is rejected (SCN-PLT-11.3), as is a token over HTTP
  (SCN-PLT-11.4).
- [x] 3.3 Rewrite `deploy/platform/trino/rules.json`:
  - the tenant-principal catalog, schema and table rules, including the metadata
    deny before the filtered `SELECT`;
  - `superset` impersonation limited to tenant principals;
  - `superset` with no privileges of its own;
  - `admin` unchanged; `idp-mcp` removed.

  Verify by running the SCN-TEN-03.x checks against the compose stack (task 5.1).
- [x] 3.4 Point `make lake-table-rows`, `make lake-table-show` and `make trino-api`
  at HTTPS 8443, with `admin` authenticating by password from the Secret. Verify
  `make lake-table-rows TABLE=gold.credit_features` still prints rows
  (SCN-PLT-11.1).

## 4. MCP server: tenant from its token

- [x] 4.1 Add `src/idp/mcp/auth.py`: a client-credentials token provider that caches
  the token until 30 s before `exp` and exposes the token's tenant. Add the
  `mcp.auth` and `trino` (`https`, `ca_cert`) config with `IDP_MCP_*` and
  `IDP_TRINO_CA_CERT` overrides, and drop `IDP_TRINO_USER`. Verify with unit tests: a
  token is reused until near expiry and then renewed (SCN-MCP-06.7); an unreachable
  or rejecting token endpoint yields the identity error (SCN-MCP-06.4); and the
  config overrides (`tests/test_config.py`).
- [x] 4.2 Switch `src/idp/mcp/sql.py` to HTTPS with `JWTAuthentication` and the CA
  certificate, using a fresh token per connection. Verify `tests/test_mcp_sql.py`
  with a fake token provider.
- [x] 4.3 In `src/idp/mcp/tools.py` and `server.py`:
  - make `tenant_id` optional, defaulting to the token's tenant;
  - refuse any other tenant before querying;
  - discover the caller's own tenant with a filtered `SELECT 1 … LIMIT 1` instead of
    `$partitions`;
  - update the tool schemas.

  Verify with `tests/test_mcp_tools.py`: defaults, refusal with no query sent
  (SCN-MCP-06.3, SCN-TEN-04.2), metacharacters (SCN-MCP-06.6), tool listing
  (SCN-MCP-06.1).
- [x] 4.4 Update `make mcp-config` to print the tenant-specific environment (client
  id and secret, token URL, `https://127.0.0.1:8085`, CA path) for
  `TENANT=<tenant>`. Verify that the printed config starts the server and a tool
  answers for that tenant on minikube (`make mcp-smoke`).
- [x] 4.5 Serving lookup tenant from the token: `GET /serving/{table}/{service_id}`
  with an optional `X-Tenant-Id` header validated against the token (required with
  auth off); README, `deploy/README.md` and the `make serve-cluster` hint updated.
  Tests pass (`tests/test_api.py`, `tests/test_auth.py`: header omitted, matching,
  mismatched 403, header without a token 401, missing header with auth off 422,
  old path 404). Verify on minikube after `make image` + `make app-deploy`: a
  `tenant_a` token reads its row, with `X-Tenant-Id: tenant_b` gets 403
  (SCN-SRV-03.4, SCN-SRV-04.1, SCN-SRV-04.4).

## 5. Leakage suite and CI

- [x] 5.1 Convert `tests/test_mcp_trino.py` to signed tokens (`mcp@tenant_a`,
  `mcp@tenant_b`) on the HTTPS stack, keeping the parity and plan checks.
  SCN-MCP-06.5 and SCN-PLT-11.2 become token-based. Verify with
  `make mcp-trino-test`.
- [x] 5.2 Add `tests/test_tenant_isolation.py` (marker `trino`):
  - storage alone: tool SQL with its predicate stripped (SCN-TEN-04.1), and the
    SCN-TEN-03.1/03.2 checks;
  - identity alone: tools against an `admin`-connected engine (SCN-TEN-04.2);
  - forged and missing tokens (SCN-TEN-04.3);
  - metadata tables (SCN-TEN-03.3);
  - a table without `tenant_id` (SCN-TEN-03.4);
  - unknown principals (SCN-TEN-03.5);
  - impersonation (SCN-TEN-03.6);
  - `SHOW STATS` per task 1.1.

  Verify with `make mcp-trino-test`.
- [ ] 5.3 Update the `mcp-trino` CI job to run the secrets generator through
  `make trino-stack`, and add `TEN` to `docs/gen_traceability.py` and the capability
  table in `docs/README.md`. Verify the job passes on a pushed branch, and regenerate
  `docs/traceability.md`.

## 6. Performance

- [x] 6.1 Add `make trino-rls-bench`: every tool's queries, 60 alternating runs
  each, as `mcp@tenant_a` (filtered) and as `admin` with the tool's own predicate
  (unfiltered), printing p50/p95 per tool and the p50 ratio (budget) and p95 ratio
  (reported). Verify it runs on the
  compose stack and on minikube.
- [x] 6.2 Run the bench on minikube with the default synthetic volume (60 alternating
  runs), record the results in design.md, and confirm every tool's median (p50)
  ratio is ≤ 1.10, with p95 reported (SCN-TEN-05.1). If a tool exceeds the budget, pause and report before tuning.
  Verify the partition-pruning assertion runs as a tenant principal
  (SCN-TEN-05.2, `test_every_scan_is_constrained_to_the_tenant`).
  Accepted as measurement noise (2026-10-09): five tools within budget (p50 ratios
  0.96–1.08); fee charges 1.21 and credit score 1.12 over, but across three runs
  their ratios were 0.94/0.79/1.21 and 1.26/0.98/1.12, on both sides of 1.0. See
  design.md, Performance results.

## 7. Documentation and rollout

- [x] 7.1 Add an ADR (identity-carried tenant to Trino, row-level security, Cube not
  adopted), and update the status of ADR-0005, ADR-0030 and ADR-0034. Update README
  1.5/1.6 (identity and storage layers), 4.6 (MCP configuration) and
  `deploy/README.md` (`trino-certs`, upgrade notes, header clients). Verify the
  links resolve and `openspec validate --specs` passes.
- [x] 7.2 Roll out on minikube following the design's migration plan (with approval
  for the cluster changes). Verify end to end: `make mcp-smoke` for both tenants,
  `make lake-table-rows`, and a `tenant_a` token reading only `tenant_a` rows.
