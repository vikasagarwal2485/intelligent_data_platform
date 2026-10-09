# Design

## Context

See proposal.md for the motivation. The current state that shapes the approach:

- **Keycloak** (`deploy/platform/keycloak/idp-realm.json`, imported at every start; in
  memory) has realm `idp` with:
  - a `tenant_id` user-attribute mapper;
  - two hard-coded users (`alice`, `bob`);
  - one client-credentials client per tenant with a hard-coded `tenant_id` mapper,
    used by machine callers of the scoring API;
  - no groups.

  Only the scoring API validates tokens (`serving/auth.py`, PyJWT against the realm's
  JWKS).
- **Trino 480** (chart 1.42.2) is coordinator-only on HTTP 8080, with file-based
  access control (`rules.json`) and no authentication: the user is whatever the
  `X-Trino-User` header says. `idp-mcp` may `SELECT` `lake.silver|gold`, and `admin`
  may do anything.
- **MCP** (`mcp/sql.py`) connects as `idp-mcp` per query. Every tool takes
  `tenant_id`, binds it as a parameter, and checks results with `scope_to_tenant`.
  Tenant discovery reads `dim_subscriber$partitions`.
- **Other Trino clients:**
  - `make lake-table-rows` / `lake-table-show` exec the Trino CLI in the pod as `admin`;
  - the compose test stack (`deploy/dev/trino-compose.yaml`) mounts the cluster's
    `rules.json`, and `tests/test_mcp_trino.py` queries as `idp-mcp`.
- **Silver and Gold tables** all carry `tenant_id` as their first partition column
  (checked against `config/avro/silver/` and the Gold builders).

## Goals / Non-Goals

**Goals:**
- One principal format that Keycloak issues, Trino authenticates, and the row filter
  understands, with no per-tenant Trino configuration.
- The MCP server never chooses its tenant: its credentials do.
- Tests that run the real Trino with real signed tokens in CI.

**Non-Goals:**
- Installing Superset (only its client, service user and impersonation rule).
- Trino's own OAuth2 web login (the Web UI is for `admin` with a password).
- Per-user Keycloak accounts for MCP end users: the MCP server is a tenant-level
  service identity. Per-user identity on MCP needs an MCP-level login, a later change.

## Decisions

### 1. The principal is `<name>@<tenant_id>`, issued as an `idp_principal` claim
- **Users:**
  - a user-attribute mapper puts the `idp_principal` attribute in the token, next to
    the existing `tenant_id` mapper;
  - provisioning (or the admin) sets both attributes, and a user belongs to their
    tenant's group;
  - the realm's **declarative user profile** marks `tenant_id` and `idp_principal` as
    editable by admins only, so users can't change their own claims
    (SCN-TEN-01.3).
- **MCP service clients** (`idp-mcp-<tenant>`): hard-coded claim mappers set
  `idp_principal = mcp@<tenant>` and `tenant_id = <tenant>`, plus an audience mapper
  for `trino`.
- **Trino** takes the user from the claim (`principal-field=idp_principal`), so the
  tenant is part of the user name and the row filter needs nothing else.
- *Rejected:*
  - **Username convention** (`alice@tenant_a` as the Keycloak username): it
    constrains usernames, and service-account users are always named
    `service-account-<client>`, so the two kinds of identity would differ.
  - **A Trino group-provider file** mapping users to tenant groups: it has to be kept
    in sync with Keycloak by hand or by a sync job, and goes stale.
  - **The tenant alone as the principal**: no per-user audit trail, and Superset
    impersonation would have to collapse users into tenants.

### 2. Trino authentication: JWT for tenants, passwords for two service users, HTTPS
`additionalConfigProperties`:
```properties
http-server.authentication.type=JWT,PASSWORD
http-server.authentication.jwt.key-file=http://keycloak.idp-platform.svc:8080/realms/idp/protocol/openid-connect/certs
http-server.authentication.jwt.required-issuer=http://keycloak.idp-platform.svc:8080/realms/idp
http-server.authentication.jwt.required-audience=trino
http-server.authentication.jwt.principal-field=idp_principal
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path=/etc/trino/tls/trino.pem
internal-communication.shared-secret=${ENV:TRINO_SHARED_SECRET}
```
- **Passwords:** a file authenticator (`password.db`, bcrypt) holds `admin` and
  `superset`, from a Secret.
- **Secrets:** `make trino-certs` creates the Secrets: a self-signed certificate (SAN
  `trino.idp-platform.svc`, `localhost`, `127.0.0.1`) with its key, the password
  file, and the shared secret. They're mounted through the chart's `secretMounts`.
- **HTTP 8080** stays open only for the chart's probes (`/v1/info` needs no
  authentication). Trino refuses credentials over HTTP (SCN-PLT-11.4).
- **EKS:** a certificate from cert-manager or ACM, and the passwords from External
  Secrets. The settings are the same.
- *Rejected:*
  - **`allow-insecure-over-http`:** it would send tokens in clear text inside the
    cluster.
  - **Trino OAuth2:** that's the browser login flow; programmatic clients only need
    JWT validation.

### 3. One row-filter rule for all tenants
`rules.json`, tables section (first match wins):
```json
{"user": "admin", "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
{"user": "[a-z0-9._-]+@[a-z0-9_]+", "catalog": "lake", "schema": "silver|gold",
 "table": ".*\\$.*", "privileges": []},
{"user": "[a-z0-9._-]+@[a-z0-9_]+", "catalog": "lake", "schema": "silver|gold",
 "privileges": ["SELECT"],
 "filter": "tenant_id = regexp_extract(current_user, '@([a-z0-9_]+)$', 1)"},
{"privileges": []}
```
- **Catalog and schema rules** grant tenant principals read-only access to `lake`
  and `silver|gold` only.
- **Impersonation:**
  `{"original_user": "superset", "new_user": "[a-z0-9._-]+@[a-z0-9_]+"}`. `superset`
  itself gets no table privileges.
- **What the rule gives:**
  - a tenant needs no rule of its own (REQ-TEN-02);
  - an unknown tenant suffix filters to zero rows (SCN-TEN-03.5);
  - a table without `tenant_id` fails analysis (SCN-TEN-03.4).
- **Metadata tables:** Iceberg's (`t$partitions`, `t$files`, …) match `.*\$.*` and
  are denied, because row filters don't apply to them.
- **Verify first:** `current_user` inside a row filter must evaluate to the querying
  (or impersonated) user, not the filter's environment user. This is the first
  task, checked on the compose stack before anything else is built on it.
- *Rejected:*
  - **One rule per tenant with a literal filter:** that's per-tenant Trino
    configuration.
  - **A view per tenant:** more objects, and it doesn't stop direct table access.

### 4. The MCP server gets its tenant from its own token
- **Token provider (`mcp/auth.py`):** calls the realm's token endpoint with the
  client-credentials grant (`IDP_MCP_CLIENT_ID`, `IDP_MCP_CLIENT_SECRET`,
  `IDP_MCP_TOKEN_URL`) and caches the token until 30 s before `exp`. The tenant is
  read from the token's `tenant_id` claim. The payload is decoded without verifying
  it here: it came straight from the identity provider over the configured channel,
  and Trino verifies it on every query.
- **Queries (`sql.py`):** connects over HTTPS with `JWTAuthentication(token)` and the
  CA certificate (`IDP_TRINO_CA_CERT`), taking a fresh token per connection.
  `IDP_TRINO_USER` is dropped.
- **Tools (`tools.py`):** `tenant_id` defaults to the token's tenant; any other value
  returns the refusal result before a query (SCN-MCP-06.3, SCN-TEN-04.2).
  Discovery becomes "does my tenant have rows" (`SELECT 1 … LIMIT 1` under the
  filter) instead of reading `$partitions`.
- **Errors:** an identity-provider failure is a new error kind, with the token
  endpoint as `source` (SCN-MCP-06.4).
- **Dev mode:** `mcp.auth.enabled=false` keeps today's behaviour (the argument is
  required), for unit tests with the fake engine only. The cluster and compose
  Trino reject unauthenticated requests anyway.
- **Issuer:** the host reaches Keycloak by port-forward, and the realm's hostname is
  pinned (`KC_HOSTNAME`, as for `make keycloak-token`), so the token's issuer always
  matches what Trino requires.

### 5. The tenant part of the realm is rendered
- `idp keycloak-realm` renders `idp-realm.json` from a base file (realm settings,
  user profile, the scoring-API and Superset clients, dev users) plus, for each
  configured tenant:
  - a group;
  - the existing scoring client `idp-tenant-<t>`;
  - the new `idp-mcp-<t>` client with its mappers.
- **Client secrets:** taken from `IDP_KC_SECRET_<TENANT>` when set. Otherwise they're
  the same documented dev defaults as today; EKS supplies real ones.
- **Check:** a test renders the realm and compares it with the committed file
  (SCN-TEN-02.1), as `make spark-manifests` does for the Spark manifests.
- *Rejected:* runtime provisioning through Keycloak's admin API. The realm is
  re-imported on every start (in-memory store), so the import file has to be the
  source of truth anyway.

### 6. Test stack: generated keys and certificates, real tokens
- `deploy/platform/trino/make_secrets.py --test-jwt` runs before `docker compose up`
  (`make trino-stack`), writing to a git-ignored directory. The same script creates
  the cluster's material for `make trino-certs`, and it uses `cryptography` and
  `bcrypt` rather than `htpasswd`, which CI runners may lack:
  - an RSA key pair (Trino reads the public key as a PEM file; it fetches a JWKS
    only by URL, which is what the cluster uses);
  - a self-signed certificate;
  - the password file;
  - the shared secret.
- Trino in the stack uses the same properties as the cluster, except that
  `jwt.key-file` points at the local PEM public key.
- **Tests** sign tokens with the private key: valid ones, and forged variants
  (unknown key, expired, wrong audience, no principal) for SCN-TEN-04.3.
- No private key is committed.

### 7. Leakage suite: disable one layer, attack the other
`tests/test_tenant_isolation.py` (marker `trino`, in the `mcp-trino` CI job):
- **Storage alone:** runs each tool's SQL with the tenant predicate stripped, as
  `mcp@tenant_a`, and asserts only `tenant_a` rows (SCN-TEN-04.1).
- **Identity alone:** runs the MCP tools with a `tenant_a` token against an engine
  connected as `admin` (no row filter), asserts the refusal for `tenant_b` and that no
  query was sent (SCN-TEN-04.2).
- **Forged tokens** (SCN-TEN-04.3), and the SCN-TEN-03.x checks on metadata tables,
  impersonation, unknown principals and a table without `tenant_id`.

### 8. Performance: same queries, filtered vs unfiltered
- `make trino-rls-bench` runs every tool's queries 60 times as `mcp@tenant_a`
  (filtered), and the same queries as `admin` with the tool's own tenant predicate
  (unfiltered), alternating the two so that drift and caching affect both. It
  reports p50 and p95 per tool.
- The budget is judged on the **median** (p50 ratio ≤ 1.10); p95 is reported, not
  gated. On the test stack the p95 ratio swung from 0.69 to 1.41 in both directions
  even over 60 runs (a laptop shared with minikube), while the medians agreed within
  that noise, so a p95 gate would measure the host, not the filter. The results are
  recorded in this design (task 6.2).
- In CI, the partition-pruning assertion runs (SCN-TEN-05.2), but not the latency
  ratio: shared runners make timing gates flaky.
- **Expected outcome:** the filter is a predicate on the leading partition column,
  so Iceberg prunes the same files as the tools' own predicate, and the overhead
  should be authentication and a cheap expression per query.

## Risks / Trade-offs

- **[`current_user` in a filter resolves to the wrong user]** → Task 1.1 proves it on
  the compose stack before anything is built on it. The fallback is a Trino group
  provider fed by the rendered realm, with one literal filter per group.
- **[Statistics leak aggregate values]** `SHOW STATS` could expose min/max and
  distinct counts across tenants → Task 1.1 also runs `SHOW STATS` as a tenant
  principal. If Trino returns table-wide statistics for a row-filtered table, the
  leakage suite gets that case and the rules are revised before continuing.
- **[Superset is trusted to say who the user is]** Anyone holding the `superset`
  password can impersonate any tenant principal → The password lives only in a
  Secret, the service user can do nothing as itself, and it is the documented trust
  boundary for the follow-up Superset change.
- **[Breaking clients]** Header-only clients stop working → The `lake-table-*`
  targets and the compose tests are updated in this change, and `make mcp-config`
  prints the new environment. `deploy/README.md` gets an upgrade note.
- **[Self-signed certificate]** Host clients must trust the generated CA →
  `make trino-certs` writes the CA to a known path, and `IDP_TRINO_CA_CERT` points to
  it.
- **[Keycloak unavailable]** New MCP tokens can't be issued → The cached token lasts
  until it expires (5 min). After that the tools return the identity error. Trino
  needs Keycloak only to fetch the realm's signing keys, not per query.
- **[Coordinator memory]** HTTPS and authentication add little; the 1536Mi limit
  stays.

## Migration Plan

1. `make trino-certs`: create the TLS, password and shared-secret Secrets.
2. `make keycloak`: apply the rendered realm (new groups, MCP clients, user
   profile), and Keycloak re-imports it.
3. `make trino-fix`: upgrade Trino with the authentication, HTTPS and the new rules.
   From this point, header-only clients are rejected.
4. Reconfigure the MCP host with `make mcp-config TENANT=<tenant>`: client
   credentials, `https://127.0.0.1:8085` and the CA path. `make trino-api` now
   forwards 8443.

**Rollback:** `helm rollback trino`, and restore `rules.json` and the previous
`mcp/sql.py` settings (`IDP_TRINO_USER`). The realm additions are harmless to leave
in place.

## Spike results (task 1.1)

Run on 2026-10-09 against Trino 480 (`trinodb/trino:480`) on the compose stack's
catalog and MinIO, with the configuration of decisions 2 and 3 and tokens signed
locally. Every check behaved as designed; nothing in the design changes.

| Check | Result |
|---|---|
| JWT with `principal-field=idp_principal` | `current_user` = `mcp@tenant_a` |
| Row filter from `current_user` | unfiltered `GROUP BY tenant_id` returns only `tenant_a` (150 of 300 rows); `WHERE tenant_id = 'tenant_b'` returns 0 |
| Filter under impersonation | `superset` as `alice@tenant_b` sees only `tenant_b`; as `admin` and as itself it is denied |
| Metadata tables | `dim_subscriber$partitions`, `credit_scores$files`: access denied |
| `SHOW STATS` | allowed, but computed under the filter: identical to `admin`'s statistics for `tenant_a` alone (minimum tenure 18, data size 5,526) and unlike the whole table's (0, 11,034), so no cross-tenant values |
| Table without `tenant_id` | fails: "Invalid row filter … Column 'tenant_id' cannot be resolved" |
| Unknown principal | no tenant suffix: "Cannot access catalog lake"; `someone@tenant_x`: 0 rows |
| Writes, Bronze | denied |
| Authentication | no credential: 401; wrong password: 401; credentials over HTTP: 403 "Authentication over HTTP is not enabled"; unknown key, expired, wrong audience, no `idp_principal`: 401 |
| Partition pruning | the scan keeps a `constraint on [tenant_id]` |

`superset` impersonation needs only the one impersonation rule; no self-impersonation
rule is required.

## Performance results (task 6.2)

`make trino-rls-bench TENANT=tenant_a` on minikube (2026-10-09, default synthetic
volume, 60 alternating runs per tool after 2 warm-up runs; filtered = `mcp@tenant_a`
with the row filter, unfiltered = `admin` with the tool's own predicate):

| Tool | filtered p50 / p95 ms | unfiltered p50 / p95 ms | p50 ratio | p95 ratio |
|---|---|---|---|---|
| analyze_subscribers_tool | 375 / 1583 | 346 / 848 | 1.082 | 1.867 |
| analyze_usage_events_tool | 265 / 751 | 270 / 732 | 0.982 | 1.025 |
| analyze_recharges_tool | 199 / 423 | 207 / 491 | 0.962 | 0.861 |
| analyze_recharge_anomalies_tool | 834 / 3309 | 850 / 3543 | 0.981 | 0.934 |
| analyze_fee_charges_tool | 241 / 895 | 199 / 1175 | **1.210** | 0.761 |
| analyze_fraud_score_tool | 802 / 2785 | 750 / 2825 | 1.068 | 0.986 |
| analyze_credit_score_tool | 758 / 2333 | 676 / 2200 | **1.121** | 1.061 |

Five tools are within the p50 budget; two are over. Across the three alternating
runs (compose stack 20 and 60 runs, minikube 60 runs) those two tools' p50 ratios
were 0.94 / 0.79 / 1.21 (fee charges) and 1.26 / 0.98 / 1.12 (credit score): on both
sides of 1.0. Isolated measurements show no row-filter cost (authentication 13 vs
15 ms; a filtered count on `fact_usage` no slower than an explicit predicate), and
every filtered scan keeps its `tenant_id` partition constraint.

**Decision (2026-10-09):** accepted as measurement noise on this host. The two
over-budget ratios are recorded above with their run-to-run spread; a re-measurement
on a quieter host (EKS or a dedicated machine) can confirm them later.
