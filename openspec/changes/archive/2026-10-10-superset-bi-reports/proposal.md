# Proposal

## Why

The platform has an analytics surface for conversational BI (the MCP tools) but none
for reports and dashboards. Apache Superset is the planned second consumer
(ADR-0038). The identity and storage layers it needs are partly in place: Keycloak
issues tenant principals, and Trino authenticates them and row-filters every lake
table, with a `superset` service user that may only impersonate tenant principals.

What's missing is Superset itself, and a tenant-safe path to Gold in TimescaleDB.
Superset reaches TimescaleDB with ordinary database credentials, and the scoring
API's `postgres` user sees every tenant, so Gold there needs isolation of its own.

## What Changes

- **Deploy Apache Superset** in `idp-platform` through the **Apache Superset
  Kubernetes Operator**, which upstream recommends in place of the now-deprecated
  Helm chart:
  - a pinned image with the Trino and Postgres drivers and the OIDC client;
  - its metadata in a `superset` database on the platform TimescaleDB;
  - no Bitnami Postgres or Redis (ADR-0017), and no Celery workers: queries run
    synchronously.
- **Sign-in only through Keycloak** (realm `idp`, client `idp-superset`,
  authorization code flow):
  - the Superset user name is the token's `idp_principal` (`<name>@<tenant>`), and
    the user's tenant is its `tenant_id`;
  - there is no local password login;
  - a user without a tenant can sign in but sees no data.
- **The tenant comes from the signed-in user, not a dashboard parameter.** The same
  dashboards serve every tenant, and each user sees only their own tenant's rows.
  There's no tenant selector, because a selectable tenant would let a user ask for
  another tenant's data.
- **Silver through Trino:**
  - Superset connects as `superset` (password) and impersonates the signed-in
    principal on every query;
  - Trino row-filters to that tenant (REQ-TEN-03), so no Superset-side filter is
    trusted.
- **Gold through TimescaleDB, isolated in the database:**
  - one login role per configured tenant (`idp_bi_<tenant>`), members of an
    `idp_bi_readers` group;
  - a row-level-security policy on every `gold.*` serving table:
    `tenant_id = <the role's tenant>`;
  - Superset swaps in the signed-in user's tenant role for each query.
  - **BREAKING (internal):** `idp publish` re-applies the grants and the policy inside
    the swap transaction (REQ-SRV-01), because the swap replaces the table.
- **No cross-tenant cache:** Superset's data cache is disabled. Two tenants opening
  the same chart run separate queries.
- **Starter content as code:**
  - datasets over Silver (`dim_subscriber`, `fact_usage`, `fact_recharge`,
    `fact_fee_charge`) and Gold (`credit_scores`, `fraud_scores`);
  - four dashboards (subscriber overview; usage overview; credit scores; fraud scores);
  - imported by `make superset-content`.
- **Tests:**
  - unit tests of the sign-in mapping and the per-tenant connection choice;
  - TimescaleDB row-level security against a real Postgres;
  - a smoke test on minikube that signs in as users of two tenants and compares
    what the same chart returns.

## Capabilities

### New Capabilities
- `bi-reports`: Superset as the tenant-aware BI surface: Keycloak sign-in and the
  user-to-tenant mapping; Silver through Trino impersonation; Gold through per-tenant
  TimescaleDB roles; no cross-tenant caching; starter datasets and dashboards as
  code.

### Modified Capabilities
- `serving-api`: REQ-SRV-01 changes: each published serving table keeps row-level
  security and the BI readers' grants across the swap.
- `platform-deployment`: a new REQ-PLT-12 for the Superset deployment (image,
  metadata database, sign-in configuration, access from the host).
- `tenant-isolation`: a new REQ-TEN-06: the BI path holds isolation in each layer,
  verified per layer like the analytics path (REQ-TEN-04).

## Non-goals

- A semantic layer (Cube) or Superset's own row-level-security rules: isolation is
  enforced by the stores, not by Superset.
- Celery, async queries, alerts and reports, thumbnails (they need Redis and
  workers).
- Superset on EKS (an overlay follows the minikube deployment).
- Cross-tenant users: someone who works for two tenants has one account per tenant.

## Impact

- **New:**
  - `deploy/platform/superset/`: the `Superset` resource, operator values, image Dockerfile,
    `superset_config.py`, the `idp_superset.py` security module, content bundle;
  - `src/idp/serving/bi_roles.py`: tenant roles and policies;
  - `make superset`, `superset-ui`, `superset-content`, `superset-smoke`,
    `bi-roles`.
- **Changed:**
  - `src/idp/serving/timescale.py`: `swap_in` re-applies RLS and grants;
  - `deploy/platform/keycloak/idp-realm.base.json`: the Superset client's redirect
    URIs and an admin group claim;
  - `deploy/platform/install-infra.sh`;
  - the TimescaleDB `superset` database.
- **Secrets:**
  - `superset-secrets`: secret key, OIDC client secret, the `superset` Trino
    password;
  - `timescaledb-bi`: one password per tenant role.
- **Docs:** README (1.2, 1.5, 1.6, a new BI section), `deploy/README.md`, an ADR.
- **Memory:** Superset adds about 1 GiB on the minikube node; `make mk-check`
  covers it.
- **Local browser access:**
  - the sign-in redirect goes to Keycloak's issuer host
    (`keycloak.idp-platform.svc:8080`), so the host needs a hosts entry and a
    forward of port 8080 while signing in (design, decision 3);
  - port 8080 is then unavailable to the Airflow and Kafka UI forwards.
