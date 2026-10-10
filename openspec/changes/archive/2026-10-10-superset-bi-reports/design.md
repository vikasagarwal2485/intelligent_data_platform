# Design

## Context

See proposal.md for the motivation. The current state that shapes the approach:

- **Keycloak** (realm `idp`, ADR-0034/0038):
  - tokens carry `idp_principal` (`<name>@<tenant>`) and `tenant_id`, from
    admin-only user attributes;
  - the base realm already holds an `idp-superset` client (authorization code,
    confidential) with mappers for both claims;
  - the issuer is pinned to `http://keycloak.idp-platform.svc:8080`
    (`KC_HOSTNAME`), so in-cluster validators (scoring API, Trino) accept tokens
    however they were requested.
- **Trino** (REQ-PLT-11, REQ-TEN-03):
  - HTTPS on 8443; JWT for tenant principals, passwords for `admin` and `superset`;
  - `superset` may impersonate only `<name>@<tenant>` principals and has no
    privileges of its own;
  - every Silver and Gold table is row-filtered to the principal's tenant.
- **TimescaleDB:**
  - database `idp`, schema `gold`, accessed as `postgres` (superuser) by the publish
    stage and the scoring API;
  - `publish` swaps each serving table in by **dropping the live table** and
    renaming a staging table inside one transaction (`timescale.swap_in`), so any
    grant or policy on the old table is lost at every publish;
  - the same server also hosts the `iceberg`, `mlflow` and `airflow` databases.
- **Platform conventions:**
  - no Bitnami charts or images (ADR-0017);
  - services' metadata in TimescaleDB databases (Airflow, MLflow);
  - images built into minikube (the Airflow DAG image);
  - the minikube node is memory-tight (`make mk-check`).

## Goals / Non-Goals

**Goals:**
- Tenant isolation on both BI paths enforced by the stores (Trino, Postgres), so
  Superset's own configuration is not the only control.
- One set of dashboards for every tenant.
- Everything reproducible: image, values, roles, content.

**Non-Goals:**
- Superset row-level-security rules, Cube.
- Async queries, Celery, Redis, alerts and reports, thumbnails.
- An EKS overlay.
- Changing Keycloak's issuer.

## Decisions

### 1. Superset through the Apache Superset Kubernetes Operator, with our own image
The Superset Helm chart is deprecated upstream: the latest, 0.22.8 for Superset 6.1.0,
is marked `deprecated: true`, and the official Kubernetes install docs point new
deployments to the Apache Superset Kubernetes Operator.

- **Operator:** chart `oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator`,
  pinned (0.3.0, CRD `superset.apache.org/v1alpha1`), installed in `idp-platform` in
  namespace-scoped mode watching only `idp-platform` (its controller requests 64 MiB).
- **One `Superset` resource** (`deploy/platform/superset/superset.yaml`):
  - `environment: Production`, so every secret must be a Secret reference:
    `secretKeyFrom`, `metastore.passwordFrom`;
  - `metastore`: structured fields, host `timescaledb.idp-platform.svc`, database
    `superset`, user `postgres`, `createDatabase: true`;
  - no `valkey`, `celeryWorker` or `celeryBeat`: the operator then renders no cache,
    broker or results backend;
  - one web server replica;
  - `lifecycle.init` with no admin user (Production rejects one; sign-in is
    Keycloak only);
  - `config`: our settings, appended after the operator's (decisions 2, 4, 7);
  - `podTemplate`: the Secrets as environment variables and the `trino-tls` CA as a
    volume.
- **Image:** `idp/superset:<content hash>`, built into minikube: `FROM
  apache/superset:6.1.0` plus `trino[sqlalchemy]`, `psycopg2-binary`, `authlib` and
  `idp_superset.py` (in `/app/idp_ext`, on `PYTHONPATH`; the operator mounts the
  config in `/app/pythonpath`). The operator runs `superset db upgrade` when the
  image's tag string changes, so the tag is a hash of the image inputs: a mutable
  `:dev` tag would skip migrations.
- **Secrets** (`superset-secrets`): `SECRET_KEY`, the `idp-superset` client secret,
  the `superset` Trino password (copied from `trino-passwords`).
- *Rejected:*
  - **the deprecated Helm chart:** unmaintained, Bitnami dependencies (ADR-0017);
  - **our own plain manifests:** we'd own the migrate and init sequencing that the
    operator provides;
  - **installing drivers at pod start:** needs the internet at every restart.
- **Alpha API:** `v1alpha1` may change between operator releases. The pin and the
  spike (task 1.1) bound that; an upgrade is a deliberate change.

### 2. Sign-in: Keycloak only; user = principal; role from a group
- `AUTH_TYPE = AUTH_OAUTH` with one provider, `keycloak`; user self-registration on
  first sign-in.
- A custom security manager maps the userinfo/ID-token claims:
  - user name = `idp_principal`;
  - names and e-mail from the profile;
  - roles: every user gets `Gamma` (the registration role: view dashboards and
    charts); a user with a tenant also gets `sql_lab` and `IdpData` (access to our
    two databases only, created with the content in task 5.1); members of the realm
    group `bi-admins` get `Admin` (a groups claim mapper is added to `idp-superset`);
  - `AUTH_ROLES_SYNC_AT_LOGIN` on, so removing a user from `bi-admins` takes effect
    at the next sign-in.
- **Untenanted users:** if `tenant_id` is missing, or isn't the principal's suffix,
  or isn't a configured tenant, the user name becomes `untenanted:<sub>`. Decision 4
  then refuses them every connection (SCN-BI-01.3).
- **The tenant user's roles** (`Gamma` + `sql_lab` + `IdpData`) give view, explore
  and read-only SQL on the two databases. DML and CSV upload are off on both
  database connections (set in the content bundle), and only Admin can create
  databases.
- Form login is not offered (`AUTH_OAUTH` has no password form).
- *Rejected:* `AUTH_REMOTE_USER` behind an OAuth proxy, which is one more component
  and still needs the claim mapping.

### 3. Browser sign-in keeps the current issuer
The browser must reach Keycloak at the issuer's host and port, because the
authorization redirect and the token's `iss` use `KC_HOSTNAME`.

Locally:
- a hosts entry `127.0.0.1 keycloak.idp-platform.svc`;
- `make superset-ui` forwards both `localhost:8088` (Superset) and `localhost:8080`
  (Keycloak). The documented cost is that the Airflow and Kafka UI forwards, which
  also use 8080, can't run at the same time.

Superset's server side uses the same URLs, because they resolve in-cluster. The
`idp-superset` redirect URIs gain `http://localhost:8088/oauth-authorized/keycloak`.

*Rejected:*
- **moving the issuer to an ingress hostname** (e.g. `keycloak.idp.local`): cleaner,
  and EKS will have a real hostname, but every validator (scoring API overlays,
  Trino's `required-issuer`, `token.sh`, the MCP token URL) would change in this
  change. That belongs with the EKS overlay.
- **Keycloak's frontend/backchannel hostname split:** it changes the issuer too.

### 4. One connection mutator decides the identity of every query
`DB_CONNECTION_MUTATOR` in `superset_config.py` calls
`idp_superset.mutate_connection(url, params, username, tenants, secrets)`. These are
pure functions in `idp_superset.py`, unit-tested without Superset:
- **The tenant** is the configured tenant whose suffix the user name carries;
  otherwise the function raises, and Superset shows an access error.
- **Trino (Silver):**
  - the URL's user stays `superset`, with password authentication;
  - the impersonated user is forced to the signed-in principal (`connect_args`),
    independent of the database's "impersonate user" switch;
  - HTTPS with the mounted `trino-tls` CA.
- **TimescaleDB (Gold):** the URL's user and password are replaced by
  `idp_bi_<tenant>` and its password from `timescaledb-bi` (mounted as environment
  variables).
- **Anything else:** refused.
- The chart and query parameters never reach this function, so nothing a user
  types can change the identity (SCN-TEN-06.2).
- *Rejected:*
  - **Superset's own "impersonate user" alone:** a flag on the database object an
    Admin can untick; the mutator enforces it regardless;
  - **one shared Postgres user plus `SET ROLE`:** a SQL Lab user could `RESET ROLE`.
    A login role per tenant has nothing to reset.

### 5. Gold isolation in Postgres: tenant login roles and one policy
- **`idp bi-roles`** (`src/idp/serving/bi_roles.py`) runs as `postgres`, idempotently:
  - creates the group role `idp_bi_readers` (NOLOGIN) and, per configured tenant,
    `idp_bi_<tenant>` (LOGIN, member of `idp_bi_readers`), with the password from
    `timescaledb-bi` (`make bi-roles` creates the Secret with random passwords if
    missing);
  - grants `CONNECT` on `idp` and `USAGE` on schema `gold` to `idp_bi_readers`;
  - revokes `CONNECT` from `PUBLIC` on every other database of the server (today
    `iceberg`, `mlflow`, `airflow`, `nessie`, `superset`, `postgres`; any added later
    too). A fixed list missed `nessie` in the spike. The services connect as
    `postgres` (superuser), so they're unaffected;
  - applies decision 6 to the existing `gold.*` tables.
- **One policy for all tenants:** `CREATE POLICY bi_tenant_rows ON gold.<t> FOR
  SELECT TO idp_bi_readers USING (tenant_id = regexp_replace(current_user,
  '^idp_bi_', ''))`. A new tenant needs a role, not a policy.
- `postgres` (publish, scoring API) is a superuser and bypasses RLS: the scoring
  API is unchanged.
- *Rejected:*
  - **a policy per tenant:** per-tenant SQL to keep in sync;
  - **a session setting** (`SET app.tenant`): settable from SQL Lab.

### 6. The publish swap carries the access controls
`timescale.swap_in` adds, before its commit:
- `ALTER TABLE … ENABLE ROW LEVEL SECURITY`;
- the `bi_tenant_rows` policy;
- `GRANT SELECT … TO idp_bi_readers`;
- `idp_bi_readers` is created if missing (a fresh database before `idp bi-roles`).

It all happens in the same transaction as the rename, under the existing advisory
lock, so no reader ever sees the new table without its policy (SCN-SRV-01.6). The
grant happens only through that transaction; default privileges can't help, because
each publish creates a new table.

### 7. No cross-tenant cache; UI state in the metadata database
- `DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}`: chart data is never cached,
  so a result computed for one user can't be served to another (REQ-BI-04).
- `FILTER_STATE_CACHE_CONFIG` and `EXPLORE_FORM_DATA_CACHE_CONFIG` use the metadata
  database (`SupersetMetastoreCache`). They hold UI state, not query results, and
  are keyed per user.
- No results backend (no async).
- *Rejected:* keying the cache by user. That depends on Superset's cache-key
  internals (version-specific), and at this data volume caching isn't needed.

### 8. Content as an exported bundle
- `deploy/platform/superset/content/` holds a Superset export bundle (YAML):
  - the two databases, without passwords: the mutator supplies credentials;
  - six table datasets plus two virtual ones (usage and overage windows), the charts,
    four dashboards.
- `make superset-content` imports it with the CLI, as a technical owner the init job
  creates (it can't sign in: no password form).
- A test checks the bundle has no configured tenant id and no secret
  (SCN-BI-05.2).
- *Rejected:* building the content through the REST API from a script: more code,
  and the export format is what Superset users edit and re-export anyway.

### 9. Tests in three places
- **Unit** (default CI job): `tests/test_bi_superset.py` imports the pure functions
  of `idp_superset.py`: claim mapping, tenant resolution, the mutator for each
  database and the refusals.
- **Postgres** (the `mcp-trino` CI job, which already starts Postgres):
  `tests/test_bi_roles.py` provisions roles in a scratch database, publishes through
  `swap_in` and checks:
  - SCN-TEN-06.1 and SCN-BI-03.2/03.3;
  - SCN-SRV-01.6, with a reader in a loop during a swap.
- **Cluster:** `make superset-smoke` signs in as `alice` (`tenant_a`) and `bob`
  (`tenant_b`) through the real Keycloak form with `requests` (following the
  redirects and posting the login form), then calls the chart-data API for each
  starter chart and compares the tenants returned (SCN-TEN-06.3, SCN-BI-04.1).

### 10. Trino: only your own queries; client metadata; key refresh
Found while building the content (task 5.1):
- **Query-text leak:** the rules of `tenant-identity-and-trino-rls` had no `queries`
  section, and Trino's file-based access control then lets every user view and kill
  every query. A tenant principal could list another tenant's query text, literals
  included, through `GET /v1/query`.
  - **Fix:** `queries`: `admin` may execute, view and kill any query; everyone else may
    only execute, and users always see their own (SCN-PLT-11.5, SCN-TEN-03.7).
- **Client metadata:** Superset validates a dataset's table through Trino's `system`
  catalog, which was denied to tenant principals.
  - **Fix:** read-only `system.jdbc` and `system.metadata` only. Trino filters both
    by the principal's table access, so Bronze stays invisible; `system.runtime` and
    the other `system` schemas stay denied (SCN-PLT-11.6).
- **Key rotation:** Keycloak's in-memory store generates new signing keys at every
  restart, and Trino loads the JWKS only at startup, so after a Keycloak restart every
  token is refused (`Unknown signing key ID`).
  - **Fix:** `make keycloak` restarts Trino after Keycloak. A database-backed Keycloak
    (stable keys) belongs with the EKS change.

## Risks / Trade-offs

- **[Superset's Trino impersonation and connect-args differ by version]** → Task 1.1
  pins the version and proves the mutator's Trino settings with a real query as an
  impersonated principal before anything else is built.
- **[The operator's API is alpha]** → Pinned chart and CRD version; the spike proves the
  fields we use (`config`, `metastore`, `podTemplate`, `lifecycle`).
- **[Port 8080 for the browser sign-in]** → Documented in `make superset-ui`; the
  issuer move to an ingress hostname is noted for the EKS change.
- **[One more ~1 GiB service on the node]** → One web replica, workers off;
  `mk-check` includes Superset; the documented fallback is scaling Superset to 0
  during heavy Spark stages, like Trino.
- **[A misconfigured `idp_bi_readers` membership grants another tenant's role]** →
  Roles come only from `idp bi-roles`; its test checks each tenant role is a member of
  `idp_bi_readers` only.
- **[The Superset Admin can edit database connections]** → Credentials come from the
  mutator, not the stored connection; an Admin can still change dashboards, which is
  the role's purpose. `bi-admins` membership is a realm decision.
- **[Revoking `CONNECT` from `PUBLIC` on other databases]** → The services connect as
  `postgres` (superuser), so they're unaffected; task 3.2 verifies Airflow, MLflow and
  the stages after the revoke.

## Migration Plan

1. `make bi-roles`: create the `timescaledb-bi` Secret, the roles and the grants, and
   apply the policy to the existing `gold.*` tables. The next `make publish` keeps
   them.
2. `make keycloak`: the realm gains the `bi-admins` group, the groups mapper and the
   redirect URI.
3. `make superset-image` then `make superset`: the operator, the Secret, the `Superset`
   resource (the operator creates the `superset` database and migrates it).
4. `make superset-content`: databases, datasets, dashboards.
5. Host setup (hosts entry), then `make superset-ui` and sign in; `make
   superset-smoke`.

**Rollback:** delete the `Superset` resource and `helm uninstall superset-operator`;
drop the `superset` database. The roles and
policies can stay: `postgres` bypasses them, and nothing else uses them.

## Spike results (task 1.1)

Run on 2026-10-09 on minikube (Kubernetes 1.35.1): Superset Kubernetes Operator 0.3.0
(chart `superset-operator`, CRD `superset.apache.org/v1alpha1`, namespace-scoped to
`idp-platform`), image `apache/superset:6.1.0` plus our drivers (`idp/superset:<hash>`),
one `Superset` resource in Production mode with the metastore on TimescaleDB
(`createDatabase: true`, 53 tables after migration), no Valkey or Celery. Lifecycle
complete; web server ready. Signed in as `alice` and `bob` through the real Keycloak
login form (scripted, inside the pod, on the registered `localhost:8088` redirect), then
SQL Lab through the API:

| Check | alice (`tenant_a`) | bob (`tenant_b`) |
|---|---|---|
| Superset user name | `alice@tenant_a` | `bob@tenant_b` |
| Silver (Trino), no filter | only `tenant_a` (150) | only `tenant_b` (150) |
| Trino `current_user` (impersonated) | `alice@tenant_a` | `bob@tenant_b` |
| Gold (TimescaleDB), no filter | only `tenant_a` (136) | only `tenant_b` (133) |
| Gold / Silver `WHERE tenant_id = <other>` | 0 / 0 | 0 / 0 |
| `SET ROLE idp_bi_<other>` | permission denied (Postgres) | permission denied (Postgres) |

Also observed: Superset's SQL Lab refuses system functions on Postgres (`current_user`,
`version`, `pg_sleep`, `pg_terminate_backend`, …), additional hardening on top of the
role. The database connections were created with no credentials in their URLs; the
mutator supplied them. Nothing in the design changes.

Findings carried into the tasks:
- **`idp bi-roles` revokes `CONNECT` on every other database:** the spike found a `nessie`
  database that a fixed list missed (decision 5).
- **SQL Lab's execute API** rejects a `json` field in the request body in 6.1.0, which
  matters for the smoke script.
- **The spike signed users in with Superset's `Admin` role**, through a temporary
  environment override. Task 2.2 replaces it with the role model (`Gamma` + `sql_lab` +
  `IdpData` for tenant users, `Admin` for `bi-admins`).
