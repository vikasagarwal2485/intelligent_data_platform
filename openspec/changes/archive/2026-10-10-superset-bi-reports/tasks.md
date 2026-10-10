# Tasks

## 1. Prove the Superset mechanics first

- [x] 1.1 Spike on minikube with a throwaway Superset (the pinned operator, a
  `Superset` resource, our image, no content):
  - Keycloak sign-in through `localhost:8088` with the hosts entry and the 8080
    forward;
  - the user name becomes `idp_principal`;
  - a Trino query impersonates the signed-in principal through the connection
    mutator and returns only that tenant;
  - a TimescaleDB query through the mutator's tenant role works.

  Pin the chart and image versions, record the results in design.md, and stop to
  revise the design if any check fails.

## 2. Superset security module

- [x] 2.1 Write `deploy/platform/superset/idp_superset.py`:
  - pure functions for the claim mapping (principal, untenanted users, roles from
    the `bi-admins` group);
  - tenant resolution from the user name and the configured tenants;
  - `mutate_connection` for Trino and TimescaleDB, refusing anything else;
  - the Superset glue (security manager subclass, the `DB_CONNECTION_MUTATOR`
    hook).

  Verify with `tests/test_bi_superset.py`: SCN-BI-01.1, SCN-BI-01.3, SCN-TEN-06.2.
- [x] 2.2 Write `deploy/platform/superset/superset_config.py`:
  - OAuth provider `keycloak` (authorize URL on the issuer host, token and JWKS URLs
    in-cluster);
  - roles and `AUTH_ROLES_SYNC_AT_LOGIN`;
  - `DATA_CACHE_CONFIG` NullCache, metastore caches for filter state and form data;
  - SQL Lab DML, CSV upload and database creation off for `IdpTenantReader`;
  - secrets from environment variables.

  Verify it imports and its settings pass a test with stubbed Superset modules.

## 3. Gold isolation in TimescaleDB

- [x] 3.1 Add `src/idp/serving/bi_roles.py` and `idp bi-roles`:
  - the group and tenant roles from the configured tenants;
  - passwords from `IDP_BI_PASSWORD_<TENANT>`;
  - the grants, the `CONNECT` revokes, and the policy on existing `gold.*` tables;
  - idempotent.

  Extend `timescale.swap_in` with RLS, the policy and the grant in the same
  transaction (decision 6). Verify with `tests/test_bi_roles.py` against the compose
  Postgres: SCN-TEN-06.1, SCN-BI-03.2, SCN-BI-03.3, SCN-SRV-01.6.
- [x] 3.2 Add `make bi-roles`: create the `timescaledb-bi` Secret if missing, then run
  `idp bi-roles` in-cluster. Verify on minikube: the roles exist, and `make publish`,
  the scoring API's `/serving` lookup, Airflow and MLflow still work after the
  `CONNECT` revokes.

## 4. Deployment

- [x] 4.1 Add `deploy/platform/superset/Dockerfile` and `make superset-image` (built
  into minikube, tagged with a hash of its inputs). Verify that the drivers and `idp_superset` import inside the image.
- [x] 4.2 Add `deploy/platform/superset/superset.yaml` (the `Superset` resource) and
  `make superset`:
  - create the `superset-secrets` Secret if missing;
  - install the pinned operator, namespace-scoped to `idp-platform`;
  - apply the resource: Production mode, metastore on TimescaleDB with
    `createDatabase`, no Valkey or Celery, `trino-tls` and `timescaledb-bi` mounted,
    one web replica sized for minikube, the image tag a content hash.

  Update `mk-check` and `install-infra.sh`. Verify SCN-PLT-12.1 and SCN-PLT-12.2.
- [x] 4.3 Update the realm base: the `bi-admins` group, a groups mapper and the
  redirect URI on `idp-superset`; re-render (`make keycloak-realm`). Verify the realm
  test passes and an admin user's token carries the group.
- [x] 4.4 Add `make superset-ui` (forwards 8088 and 8080) and document the hosts
  entry. Verify a browser sign-in as `alice` reaches the Superset home page (SCN-BI-01.1)
  and the form login is refused (SCN-BI-01.2).
  Verified with the host-side flow a browser makes (`localhost:8088` -> Keycloak's form
  at `keycloak.idp-platform.svc:8080` -> back to Superset as `alice@tenant_a`), with
  the issuer host resolved to 127.0.0.1 as the hosts entry does; a check in a real
  browser needs the operator's hosts entry. A password POST to Superset's login is
  refused (405).
- [x] 4.5 Close the query-text leak found while building the content, and let BI
  clients read table metadata (`rules.json`):
  - a `queries` section: only `admin` views or kills others' queries;
  - read-only `system.jdbc` and `system.metadata` for tenant principals (filtered by
    table access; `system.runtime` denied);
  - `make keycloak` restarts Trino, which loads Keycloak's keys only at startup while
    the in-memory Keycloak gets new keys at every restart.

  Verified with `tests/test_tenant_isolation.py` (84 Trino-marked tests pass) and on
  minikube: `mcp@tenant_a` no longer lists `mcp@tenant_b`'s query; token sign-in to
  Trino works after a Keycloak restart (SCN-PLT-11.5, SCN-PLT-11.6, SCN-TEN-03.7).

## 5. Content

- [x] 5.1 Build the two starter dashboards and their six datasets on the two
  connections, export the bundle to `deploy/platform/superset/content/`, and add
  `make superset-content`. Verify the import into a freshly installed Superset
  (SCN-BI-05.1) and a test that the bundle has no tenant id and no secret
  (SCN-BI-05.2).

## 6. End-to-end and CI

- [x] 6.1 Add `make superset-smoke`: sign in as `alice` and `bob` through the Keycloak
  form and compare every starter chart's data. Also run the SQL Lab statements of
  SCN-BI-02.2 and SCN-BI-03.2 through the API. Verify on minikube: SCN-BI-02.1,
  SCN-BI-03.1, SCN-BI-04.1, SCN-TEN-06.3.
- [ ] 6.2 Run `tests/test_bi_roles.py` in the `mcp-trino` CI job and the unit tests in
  the default job; add `BI` to `docs/gen_traceability.py` and `docs/README.md`.
  Verify both jobs pass on a pushed branch.

## 7. Documentation

- [x] 7.1 Add an ADR (Superset as the BI surface, isolation in the stores, the
  connection mutator, per-tenant Postgres roles). Update README 1.2/1.5/1.6 and a new
  BI section (sign-in, host setup, dashboards, isolation), and `deploy/README.md`
  (install, secrets, troubleshooting). Verify the links resolve and
  `openspec validate --specs` passes.
