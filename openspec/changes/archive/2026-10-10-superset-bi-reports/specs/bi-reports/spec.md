# BI reports delta

## Purpose

Apache Superset as the tenant-aware BI surface over the lake: users sign in through
Keycloak, and every chart and SQL query reads only the signed-in user's tenant, from
Silver through Trino and from Gold through TimescaleDB.

## ADDED Requirements

### Requirement: REQ-BI-01 — Sign-in through Keycloak; the user's tenant from the token
Superset SHALL authenticate users only through the platform realm (OIDC
authorization code flow, client `idp-superset`); local password sign-in SHALL be
disabled for users. On sign-in:
- the Superset user name SHALL be the token's `idp_principal` (`<name>@<tenant>`);
- the user's tenant SHALL be the token's `tenant_id`, equal to the principal's
  suffix;
- members of the realm group `bi-admins` SHALL get Superset's Admin role, and every
  other user a read-only role that can view dashboards and charts, explore, and run
  read-only SQL.

The tenant SHALL come only from the token: no dashboard filter, URL parameter or
request header can change it. A user whose token has no tenant, or whose principal
and tenant disagree, SHALL be able to sign in but SHALL get no data from any
database.

#### Scenario: SCN-BI-01.1 — Sign-in maps the principal and tenant
- **WHEN** `alice` (tenant `tenant_a`) signs in through Keycloak
- **THEN** her Superset user is `alice@tenant_a`, with tenant `tenant_a` and the
  read-only role

#### Scenario: SCN-BI-01.2 — No local sign-in
- **WHEN** a client posts a user name and password to Superset's own login endpoint
- **THEN** it is refused, and only the Keycloak sign-in is offered

#### Scenario: SCN-BI-01.3 — No tenant, no data
- **WHEN** a signed-in user's token carries no `tenant_id`
- **THEN** every chart and SQL query they run fails with an access error and returns
  no rows

### Requirement: REQ-BI-02 — Silver through Trino as the signed-in principal
Superset SHALL query Silver through Trino's `lake` catalog over HTTPS, authenticated
as the `superset` service user, impersonating the signed-in user's principal on
every query, so that Trino's row-level security (REQ-TEN-03) limits each query to
that user's tenant. Superset SHALL NOT read the lake as itself.

#### Scenario: SCN-BI-02.1 — A Silver chart shows one tenant
- **WHEN** users of `tenant_a` and `tenant_b` open the same chart over
  `silver.dim_subscriber` grouped by `tenant_id`
- **THEN** each sees exactly one group, their own tenant

#### Scenario: SCN-BI-02.2 — SQL can't widen the scope
- **WHEN** a `tenant_a` user runs `SELECT count(*) FROM silver.fact_usage WHERE
  tenant_id = 'tenant_b'` in SQL Lab against the Trino database
- **THEN** the count is 0

### Requirement: REQ-BI-03 — Gold through TimescaleDB as the user's tenant role
Superset SHALL query the Gold serving tables (`gold.*` in TimescaleDB) as the
signed-in user's tenant role: one database login role per configured tenant
(`idp_bi_<tenant>`), chosen per query from the user's tenant, never from the user's
input. Each role SHALL be able to read only the `gold` schema of the serving
database, through a row-level-security policy that returns only rows whose
`tenant_id` is the role's tenant, and SHALL NOT be able to change roles or read
other databases on the server (catalog, MLflow, Airflow, Superset metadata). The
roles and their passwords SHALL be provisioned from the configured tenants.

#### Scenario: SCN-BI-03.1 — A Gold chart shows one tenant
- **WHEN** users of `tenant_a` and `tenant_b` open the same chart over
  `gold.credit_scores`
- **THEN** each sees only their own tenant's rows

#### Scenario: SCN-BI-03.2 — The role can't escape
- **WHEN** a `tenant_a` user runs, in SQL Lab against the TimescaleDB database,
  `SELECT count(*) FROM gold.fraud_scores WHERE tenant_id = 'tenant_b'`, then
  `SET ROLE idp_bi_tenant_b`, then a query of another database's tables
- **THEN** the count is 0, and the other two are refused

#### Scenario: SCN-BI-03.3 — Adding a tenant
- **WHEN** a tenant is added to the configuration and the roles are re-provisioned
- **THEN** its role exists, reads only its rows, and no policy or Superset setting is
  edited by hand

### Requirement: REQ-BI-04 — No result is shared across tenants
Superset SHALL NOT serve a cached query result to a user of another tenant: query
results SHALL NOT be cached across users, so two users opening the same chart always
get results computed under their own identity.

#### Scenario: SCN-BI-04.1 — Same chart, back to back
- **WHEN** a `tenant_a` user opens a chart and a `tenant_b` user opens the same chart
  right after
- **THEN** the second result is `tenant_b`'s, computed for that user, not the first
  user's

### Requirement: REQ-BI-05 — Starter datasets and dashboards as code
The repository SHALL hold Superset content that can be imported into a fresh
deployment with one command:
- the two database connections (Trino `lake`, TimescaleDB serving) without secrets;
- datasets over `silver.dim_subscriber`, `silver.fact_usage`, `silver.fact_recharge`,
  `silver.fact_fee_charge`, `gold.credit_scores` and `gold.fraud_scores`, and virtual
  datasets over `silver.fact_usage` and `silver.fact_fee_charge` for time windows;
- "Subscriber overview" and "Usage overview" dashboards (Silver), and "Credit scores"
  and "Fraud scores" dashboards (Gold).

The content SHALL contain no tenant literal: the same dashboards serve every tenant.

#### Scenario: SCN-BI-05.1 — Import into a fresh Superset
- **WHEN** the content is imported into a Superset with no dashboards
- **THEN** the four dashboards, their charts and their datasets exist, and a signed-in
  tenant user can open them

#### Scenario: SCN-BI-05.2 — No tenant in the content
- **WHEN** the content files are searched for configured tenant ids
- **THEN** none is found
