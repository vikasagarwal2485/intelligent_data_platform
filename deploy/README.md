# Deployment

Kubernetes-native on both tiers (per the reviewed plan): **local = minikube**,
**production = AWS EKS**. The same Kustomize base is used by both overlays.

The directory is split by **owning team** (separation of concerns, ADR-0018):
`platform/` holds the shared infrastructure the **platform team** maintains;
`app/` holds the workloads the **application team** deploys onto it.

```
deploy/
  platform/                 # PLATFORM TEAM — shared infrastructure
    base/                   #   namespaces, RBAC, per-tenant isolation (Kustomize base)
    overlays/local/         #   minikube base overlay (namespaces/RBAC/isolation)
    overlays/aws/           #   EKS base overlay (namespaces/RBAC/isolation)
    argocd/                 #   GitOps Applications for EKS (platform base + app)
    kafka/                  #   Strimzi Kafka cluster, schema registry, topics, admin UI
    keycloak/  timescaledb/ #   per-service plain manifests
    trino/  nessie/         #   per-service tuned Helm values; trino/ also holds the `lake`
                            #   catalog (lake.properties) and access rules (rules.json)
    airflow/                #   Airflow values, RBAC, DAG image Dockerfile
    monitoring/             #   Prometheus/Grafana values, dashboards, Kafka PodMonitor
    install-infra.sh        #   one-shot platform install (Helm repos + services)
    strimzi-tune.sh  trino-tune.sh  nessie-tune.sh   # self-healing repair scripts
  dev/                      # local test stacks: trino-compose.yaml (Trino + Postgres + MinIO
                            #   for the MCP integration tests, `make mcp-trino-test`)
  app/                      # APPLICATION TEAM — workloads on the platform
    deployment/             #   scoring-api base (Deployment/Service + kustomization)
    overlays/local/         #   scoring-api patch: dev image, imagePullPolicy Never, 1 replica
    overlays/aws/           #   scoring-api patch: ECR image, IRSA, 3 replicas
    jobs/                   #   produce/consume/silver/gold/score/labels/train/publish Jobs
```

## What the platform base provides
- **Multi-tenancy & isolation** (`platform/base/isolation.yaml`): per-tenant
  `ResourceQuota` and a default-deny `NetworkPolicy` that blocks cross-tenant traffic.
- **RBAC** (`platform/base/rbac.yaml`): namespace-scoped roles (`ds-credit`,
  `ds-fraud`, `analyst`) bound to Keycloak OIDC group claims.

## What the app provides
- **Scoring API** (`app/deployment/scoring-api.yaml`): the FastAPI credit/fraud
  service — owned by the app team. The base is environment-agnostic; the
  `app/overlays/{local,aws}` patch the image/replicas/pull-policy. Deployed
  separately from the platform base (`make app-deploy`), so the two teams own
  and roll out their pieces independently.

## Local (minikube)
```bash
make mk-up          # start minikube with ingress/metrics addons
make image          # build idp/scoring-api:dev INTO minikube's docker daemon
make platform-base  # kubectl apply -k deploy/platform/overlays/local  (namespaces/RBAC/isolation)
make app-deploy     # apply -k deploy/app/overlays/local + rollout restart (scoring-api)
```

> **Re-deploying after a code change:** rebuild with `make image`, then run
> `make app-deploy` again. Because the dev image keeps the same tag
> (`idp/scoring-api:dev`), `kubectl apply` alone sees no change and won't restart
> the pods; `app-deploy` therefore issues a `kubectl rollout restart` so new pods
> pick up the freshly built image (and waits for the rollout to complete).

> **Why `make image` first?** The scoring API runs a project image that is not
> published to any registry. For local dev you build it directly into minikube's
> Docker daemon (`eval $(minikube docker-env) && docker build ...`) and the local
> overlay sets `imagePullPolicy: Never`. Skipping this causes the pod to try to
> pull `idp/scoring-api:dev` from Docker Hub and fail with `ImagePullBackOff`.
> On EKS the image instead comes from ECR (`app/overlays/aws`).

Platform infrastructure (Apache Kafka via Strimzi, Nessie + Iceberg, MinIO,
Trino, TimescaleDB, Keycloak, OPA, Airflow, MLflow, Prometheus/Grafana) is
installed by [`platform/install-infra.sh`](platform/install-infra.sh):

```bash
make platform-services        # or: bash deploy/platform/install-infra.sh idp-platform
```

The script adds the required Helm repos before installing — omitting
`helm repo add` is what produces `Error: repo <x> not found`. It deliberately
**avoids Bitnami charts** (whose free images were removed on 2025-08-28, causing
`ImagePullBackOff` on e.g. `bitnami/minio:...`). Sources used:

| Component | Source | Image |
|-----------|--------|-------|
| MinIO | `https://charts.min.io/` (official) | `quay.io/minio/minio` |
| TimescaleDB | plain manifest `platform/timescaledb/timescaledb.yaml` | `timescale/timescaledb` |
| Keycloak | plain manifest `platform/keycloak/keycloak.yaml` | `quay.io/keycloak/keycloak` |
| Apache Kafka (Strimzi) | `https://strimzi.io/charts/` | `quay.io/strimzi/*` |
| Nessie | `https://charts.projectnessie.org` | `ghcr.io/projectnessie/*` |
| Trino | `https://trinodb.github.io/charts` | `trinodb/trino` |
| Airflow | `https://airflow.apache.org` | `apache/airflow` |
| MLflow | `https://community-charts.github.io/helm-charts` | upstream |
| Prometheus + Grafana | `https://prometheus-community.github.io/helm-charts` | `quay.io/prometheus/*` |

All images are freely pullable — no Bitnami subscription needed. The script is
idempotent and first removes any earlier broken Bitnami releases.

## Spark stages as SparkApplications (Spark Operator)

Silver, Gold, Score, publish and maintenance run in the cluster as **Kubeflow Spark
Operator** `SparkApplication`s: one driver pod, which starts its executor pods, all
from the `idp` image (ADR-0029). labels, train and the streaming stages stay
Kubernetes Jobs.

- **Install (idempotent):** `make spark-operator`, also run by
  `install-infra.sh`.
  - It pins chart `2.5.2` in `idp-platform`, watching only `idp-app`.
  - It creates the driver ServiceAccount `idp-spark`, with RBAC to manage its own
    executors.
  - It adds the Airflow Role `idp-spark-submitter` (create/watch/delete
    `sparkapplications`).
- **Profiles:** `spark.profiles.<stage>` in `config/platform.yaml` sets driver and
  executor cores, memory (the JVM heap), `memoryOverhead` and executor
  `instances`. Each pod's memory limit is heap plus overhead. The defaults fit the
  12 GB minikube node.
- **Run a stage:** `make silver` (also `gold`, `score`, `publish`, `maintain` and
  `silver-full-refresh`). The target:
  1. runs `mk-check`
  2. renders the manifest (`idp spark-manifest <stage>`) and applies it
  3. waits for `COMPLETED` or `FAILED`, and prints the driver log

  Pods: `kubectl -n idp-app get pods -l spark-role` (the driver and
  `…-exec-N` executors).
- **Scale a stage:** raise `executor.instances` (or `memory`) in its profile, run
  `make mk-check` (does the node still fit it?), then `make spark-manifests` (it
  re-renders `deploy/app/spark/*.yaml` and the DAG image's copy, which a test
  compares). No code changes.
- **Airflow:** the DAG image carries the rendered manifests (`dags/spark/`), and its
  Spark tasks use `SparkKubernetesOperator`. After changing a profile, run
  `make spark-manifests` and then `make airflow-reload`.
- **The image:** Spark on Kubernetes starts pods with `driver …` / `executor …`,
  and the image's entrypoint (`docker/spark-entrypoint.sh`) handles those. Every
  other command (`idp serve`, `idp consume`, …) runs unchanged.
- **Troubleshooting a failed application:**
  - `kubectl -n idp-app get sparkapplication idp-<stage> -o yaml` (see
    `.status.applicationState`)
  - `kubectl -n idp-app logs idp-<stage>-driver`
  - An executor OOMKilled below its limit means the node cap is too small; see
    `make mk-check`.
- **Rollback:** the single-pod Jobs are still in `deploy/app/jobs/`, e.g.
  `kubectl -n idp-app apply -f deploy/app/jobs/gold-job.yaml`.

## Streaming ingestion: in-memory or through the landing zone

Two ways to fill Bronze from Kafka. Both end in the same Bronze for the same data.

| Target | Steps (Jobs in `idp-app`) |
|---|---|
| `make stream` | `idp-produce` (generates in memory → Kafka) → `idp-consume` (→ Bronze) |
| `make stream-from-landing` | `idp-generate` (→ `s3://idp-landing`) → `idp-produce-from-landing` (landing files → Kafka) → `idp-consume` |
| `make stream-replay` | `idp-produce-from-landing` → `idp-consume`: re-publish the landing files already there |
| `make generate-job` | `idp-generate` only |

- **The `idp-landing` bucket** holds the source files, laid out as
  `tenant=<id>/<entity>.json`. It's created by `install-infra.sh` and
  `make minio-mkbuckets`, and emptied by `make minio-reset` (which also clears the
  lake's Iceberg catalog, see below). It's not a lake layer,
  so Bronze archiving and maintenance never touch it.
- **Each step waits for the previous one,** and the chain **stops at the first
  failed Job** without sitting out a timeout.
- **A rejected landing file fails the chain:** its folder names a tenant that isn't
  configured, it isn't a known entity, it isn't a JSON array, or any record belongs
  to another tenant. The produce Job publishes the good files, lists the rejected
  ones in its log, and exits 1, so consume doesn't run. Fix or remove the file,
  then `make stream-replay`.
- **Keep `IDP_SUBSCRIBERS`/`IDP_DAYS` equal** in `generate-job.yaml`,
  `produce-job.yaml` and `labels-job.yaml`, because labels are regenerated from
  them.
- **Run one streaming target at a time:** they share the Kafka topics and the
  `idp-consume` Job.

## The Iceberg lake (JDBC catalog)

Bronze, Silver and Gold are Iceberg tables (ADR-0028), catalogued in an **Iceberg
JDBC catalog**: the `iceberg` database on the platform TimescaleDB/Postgres.
Every stage pod gets the catalog from `IDP_CATALOG_TYPE=sql` and
`IDP_CATALOG_URI=postgresql+psycopg2://…@timescaledb.idp-platform.svc:5432/iceberg`.

- **Catalog database.** `make lake-catalog` (also run by `install-infra.sh`)
  creates the `iceberg` database idempotently. The first stage creates the catalog
  tables. Tables live at `s3://idp-<layer>/tables/<table>`, and each layer
  namespace carries that location too.
- **Look inside the catalog (read-only):**
  - `make lake-catalog-show` lists every table's current `metadata.json` and the
    namespace locations. That's all the catalog database stores.
  - `make lake-table-show TABLE=bronze.accounts` decodes that table's
    `metadata.json` from MinIO: schema, partitioning, properties and snapshots.
  - `make lake-catalog-psql` opens a SQL shell on the `iceberg` database.
- **Resetting the lake is buckets plus catalog.** The tables are registered in the
  `iceberg` database, not in MinIO. `make minio-reset` recreates the buckets **and**
  clears the catalog entries (`lake-catalog.sh --reset`). Never empty the lake
  buckets any other way (`mc rb`, the MinIO console) without also running
  `bash deploy/platform/timescaledb/lake-catalog.sh idp-platform --reset`. See
  Troubleshooting.
- **Why not Nessie.** Nessie's REST catalog keeps only a table's current snapshot,
  and the incremental Silver checkpoints need the Bronze snapshot history. Nessie
  stays provisioned (in-memory) but is not on the lake path.
- **Incremental Silver.** `make silver` processes only Bronze that is new since the
  last run; its checkpoints live in `s3://idp-silver/_checkpoints/`.
  `make silver-full-refresh` resets them and rebuilds the facts from all of Bronze.
- **Maintenance.** The `idp_maintenance` DAG, daily at 03:00, or `make maintain`
  compacts files, expires snapshots older than `lake.snapshot_retention_days`, and
  removes orphan files. Expiry never removes a Bronze snapshot that a Silver
  checkpoint still needs, and Bronze records are never deleted.
- **Bronze archive policy.** `make lake-archive-policy` renders the lifecycle rule:
  objects under `tables/` older than `lake.bronze_archive_days` move to a colder
  class. MinIO rejects a transition to an unknown tier ("Invalid storage class"),
  so on minikube the rule is **rendered only**. To apply it, configure a remote
  tier (`mc ilm tier add ...`) and re-run with `ARCHIVE_TIER=<name>`. On EKS,
  apply `idp archive-policy --format s3` with
  `aws s3api put-bucket-lifecycle-configuration` (S3 Glacier Instant Retrieval
  keeps objects readable for a full refresh).

### Migrating a pre-Iceberg (Parquet) lake
```bash
make lake-catalog               # the `iceberg` catalog database
make image && make app-deploy && make airflow-reload
make migrate-lake               # Parquet Bronze -> Iceberg Bronze (re-runs skip migrated entities)
make silver-full-refresh && make gold && make score && make publish
```
- `idp migrate-lake` appends each legacy `s3://idp-bronze/<entity>` to
  `bronze.<entity>` in one commit, and marks the table (`idp.migrated-from`).
- Silver and Gold are rebuilt, not migrated.
- The old Parquet score history isn't carried over, so the MCP score windows
  restart from the migration.
- The legacy Parquet paths are left untouched, and orphan cleanup only scans
  `tables/`. Remove them once the Iceberg lake checks out.
- Rollback: redeploy the previous image, which reads the untouched Parquet.

## Production (AWS EKS)
```bash
kubectl apply -k deploy/platform/overlays/aws        # platform base (namespaces/RBAC)
kubectl apply -k deploy/app/overlays/aws             # scoring-api app
# or let Argo CD sync both (platform base then app, via sync-waves):
kubectl apply -f deploy/platform/argocd/application.yaml
```
EKS specifics: S3 (per-tenant prefixes + KMS), IRSA for pod S3 access, External
Secrets for credentials, ECR for images, ALB ingress.

## Troubleshooting

**Strimzi operator pod not Ready** (`Liveness/Readiness probe failed: .../healthy
context deadline exceeded` or `connection refused`):
```bash
make strimzi-fix
```
This widens the operator's probe timeouts/initialDelay and raises its memory
limit **via Helm** (so Helm remains the single field owner). Do **not** use
`kubectl patch` for this — it creates a second field owner and later
`helm upgrade` fails with `conflicts with "kubectl-patch" ...
livenessProbe.initialDelaySeconds`. If you already hit that conflict, reinstall
cleanly: `helm uninstall strimzi -n idp-platform` then re-run `make strimzi-fix`.
The underlying cause is usually node resource pressure — if it still won't go
Ready, give minikube more headroom:
`minikube stop && minikube start --cpus=8 --memory=16384`.

**Apiserver flapping** (`TLS handshake timeout`, `connection refused` to the
API, SA-token mount failures): the cluster is overloaded or kubeconfig is stale.
Run `minikube status`, `minikube update-context`, and increase resources.

**Trino coordinator CrashLoop / OOM** (`container not running`, `connection
reset by peer`, startup/liveness probe failures): the chart defaults to an 8G
heap + worker pods, too big for a small node.
```bash
make trino-fix        # coordinator-only, 768M heap, auto-recovers stuck releases
```

**Trino and the MCP tools.** Trino serves the MCP analytics tools (ADR-0030). Its
`lake` catalog is an Iceberg JDBC catalog on the TimescaleDB `iceberg` database
(catalog name `lake`, the rows the stages commit) with native S3 to MinIO:
`deploy/platform/trino/lake.properties`, credentials from the `timescaledb` and
`minio` Secrets. File-based access control (`deploy/platform/trino/rules.json`)
gives the MCP user `idp-mcp` `SELECT` on `lake.silver` and `lake.gold` only; `admin`
may do anything, every other user nothing. `make trino-fix` installs both files
(`--set-file`), and `make trino-api` forwards Trino to `127.0.0.1:8085` for the host
MCP server. Check it:
```bash
make trino-api &                                   # 127.0.0.1:8085
make mcp-smoke                                     # every tool, both tenants
```
- *`Access Denied: Cannot access catalog lake`* — the client isn't `idp-mcp`
  (or `admin`); check `IDP_TRINO_USER`.
- *`relation "iceberg_tables" does not exist`* — the catalog database is empty; run
  a stage first (the stages create the catalog tables), or `make lake-catalog`.
- *`TABLE_NOT_FOUND` / empty results after a catalog change* — the rows must carry
  `catalog_name = lake` (`make lake-catalog-show`; `make lake-catalog-rename` once).
- **Memory.** The coordinator uses about 0.9 GB. On the 12 GB minikube node, run
  `make mk-check` before a Spark stage with Trino up; if it fails, scale Trino down
  for the stage (`kubectl -n idp-platform scale deploy/trino-coordinator --replicas=0`)
  or raise the node cap.

**MLflow model registry** (REQ-ML-04). `make mlflow` installs community-charts/mlflow
1.11.2 (MLflow 3.14.0, matching the `mlflow-skinny` client) from
`deploy/platform/mlflow/mlflow-values.yaml`, after creating its backend database
`mlflow` on TimescaleDB. Artifacts go to `s3://idp-ml/mlflow` on MinIO; clients upload
and download them directly with their `IDP_S3_*` keys (not proxied).
- **Endpoint:** Service `mlflow` port 80 (container 5000), so in-cluster
  `http://mlflow.idp-platform.svc:80`. That is `IDP_MLFLOW_TRACKING_URI` on the train
  and score Jobs, the SparkApplications, the Airflow stage pods and the scoring API.
  `make mlflow-ui` forwards it to http://127.0.0.1:5050 (port 5000 is often taken by
  macOS AirPlay).
- **What to look for:** experiment `idp-training` (one run per `train`, with both
  models' hold-out metrics and the `idp.version`/`idp.git_sha` tags); registered models
  `idp-credit` and `idp-fraud`, each with a `champion` alias on the latest version that
  passed its gate (`credit.min_auc`, `fraud.min_pr_auc_lift`). A version that failed is
  registered with `gate=failed` and doesn't move the alias. To roll back, move
  `champion` to an older version in the UI; the scoring API follows within
  `mlflow.refresh_seconds`, and the next Score run uses it.
- **OOMKilled at startup:** MLflow 3 starts a job runner with about ten huey
  consumers (~200 MiB each) unless `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`, which
  the values set.
- **Connections:** TimescaleDB allows 25; the values cap MLflow's pool at 2 (+2
  overflow) with one worker.
- **Fallback:** unset `IDP_MLFLOW_TRACKING_URI` and the workloads use the joblib files
  in `s3://idp-ml` again (the latest trained model, no gate).

**Nessie CrashLoopBackOff**: the chart can default to a version store needing an
external DB, or OOM on too small a limit.
```bash
make nessie-fix       # in-memory store + more memory; check logs first:
kubectl -n idp-platform logs deploy/nessie --previous --tail=40
```

**Spark stage pods `OOMKilled` far below their limit** (Silver/Gold/maintain): with
the Docker driver the minikube node is a container with its own memory cap, which
Kubernetes doesn't see (`kubectl describe node` can report 21 GB while the cap is
6 GB). When the node container hits its cap, the kernel kills the largest
process, the Spark JVM. Check it and raise it live (no restart, state kept):
```bash
docker inspect minikube --format '{{.HostConfig.Memory}}'   # bytes; 0 = no cap
docker update --memory 12g --memory-swap 12g minikube
```
`make mk-check` does this check for you before you run Spark stages. It compares
the container's cap with what is running now plus the largest Spark stage profile
(`spark.profiles` in `config/platform.yaml`, driver plus executors, heap plus
overhead). It fails with the exact `docker update` command when they don't fit.
Re-run it after raising a profile's executors.

**Stages fail with `FileNotFoundError: … /metadata/000NN-….metadata.json`**
(e.g. `idp consume` in `to_bronze`, or `idp silver`): the lake buckets were emptied
but the Iceberg catalog still lists the tables, so it points at metadata files that
no longer exist. Typically this happens after a bucket reset done without the
catalog, such as a `make minio-reset` from before the catalog step existed, or
`mc rb`. Confirm and fix:
```bash
make minio-ls                                   # buckets empty ...
kubectl -n idp-platform exec deploy/timescaledb -- psql -U postgres -d iceberg \
  -c "select table_namespace, table_name from iceberg_tables"   # ... but tables still listed
make minio-reset                                # recreate buckets + clear the catalog, together
```
The data is already gone, so clearing the catalog loses nothing. The next
`make stream` (or `stream-from-landing`) recreates the tables, and the next
`make silver` starts from all of Bronze.

**Strimzi entity-operator `OOMKilled` (exit 137)** (the `*-entity-operator` pod's
topic-operator/user-operator containers): the Kafka CR now gives the topic
operator explicit memory and drops the unused user operator — re-apply with
`make kafka`. This doesn't block anything: topics already exist and produce/consume
work while it restarts; the entity operator only reconciles `KafkaTopic` CRs.

**Keycloak `OOMKilled` (exit 137), CrashLoopBackOff**: the Quarkus JVM sized its
heap from the cgroup limit and total RSS exceeded the memory limit. The manifest
caps it with `JAVA_OPTS_APPEND=-XX:MaxRAMPercentage=50.0 -XX:MaxMetaspaceSize=256m`;
re-apply it with `make keycloak`. Keycloak is **required** by the scoring API in the
cluster (it issues the tokens, and the API fetches its signing keys): with Keycloak
scaled down, scoring calls get `503` (auth backend unavailable) once the API's key
cache expires.

### Calling the scoring API (Keycloak tokens, REQ-SRV-03)

**Breaking change:** in the cluster (both overlays) the scoring API requires a
Bearer token on every endpoint except `GET /health`; an unauthenticated call gets
`401`. Local `make serve` is unchanged (auth off by default).

**The realm.** `make keycloak` loads `deploy/platform/keycloak/idp-realm.json` into
the ConfigMap `keycloak-realm` and restarts Keycloak, which imports realm `idp` at
every start (its dev store is in-container, so the import re-applies after each
restart). `KC_HOSTNAME=http://keycloak.idp-platform.svc:8080` pins the issuer, so a
token has `iss = http://keycloak.idp-platform.svc:8080/realms/idp` however it was
requested, which is what the API expects (`IDP_AUTH_ISSUER`). The realm holds (dev
secrets and passwords, never for EKS):

| Client / user | For | Token's `tenant_id` |
|---|---|---|
| `idp-tenant-a` / `idp-tenant-b` (client credentials; secrets `tenant-a-dev-secret` / `tenant-b-dev-secret`) | machine callers of one tenant | hard-coded `tenant_a` / `tenant_b` |
| `idp-dev-cli` (public, password grant) with users `alice` / `bob` (password `dev-password`) | people | the user's `tenant_id` attribute (`tenant_a` / `tenant_b`) |
| `idp-scoring-api` | the API itself (the audience every token above carries) | — |

**Get a token and call each endpoint:**
```bash
TOKEN=$(make -s keycloak-token TENANT=tenant_a)      # client credentials of idp-tenant-a
kubectl -n idp-app port-forward svc/scoring-api 18000:80 &
AUTH="Authorization: Bearer $TOKEN"

curl -s localhost:18000/health                                               # open
curl -s -X POST localhost:18000/credit-limit -H "$AUTH" -H 'content-type: application/json' \
  -d '{"service_id":"tok_demo","features":{"recharge_avg_amount":40,"account_tenure_days":500}}'
curl -s -X POST localhost:18000/fraud-score -H "$AUTH" -H 'content-type: application/json' \
  -d '{"service_id":"tok_demo","features":{}}'
curl -s localhost:18000/serving/credit_scores/tenant_a/<service_id> -H "$AUTH"
```
The body's `tenant_id` can be omitted (the token's is used); if given it must match.
A person's token instead (e.g. `alice`, tenant_a):
```bash
kubectl -n idp-platform port-forward svc/keycloak 18180:8080 &
curl -s -d grant_type=password -d client_id=idp-dev-cli -d username=alice -d password=dev-password \
  http://127.0.0.1:18180/realms/idp/protocol/openid-connect/token | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
```
Tokens live 5 minutes. **Responses:** `401` no, expired or forged token; `403`
token without `tenant_id`, or a body/path tenant other than the token's; `503` models
not trained, TimescaleDB off, or Keycloak unreachable.

**Rollback (dev only, never on EKS):** set `IDP_AUTH_ENABLED=false` on the
`scoring-api` Deployment. The aws overlay expects the production Keycloak's issuer
(`https://<KEYCLOAK_HOST>/realms/idp`) and real client secrets from a Secret.

**Postgres/TimescaleDB `PANIC: could not locate a valid checkpoint record`**: WAL
corruption from an ungraceful kill (usually a node OOM). The DB holds only
regenerable data here (Airflow metadata + `gold.*` serving copies), so the clean
fix is to reset its volume: `kubectl -n idp-platform delete deploy timescaledb &&
kubectl -n idp-platform delete pvc timescaledb-data && kubectl apply -n idp-platform
-f deploy/platform/timescaledb/timescaledb.yaml`. Give the node more memory to
avoid repeat OOMs (`minikube start --memory=20480`).

**Upgrading to the single Spark engine (ADR-0027)** — the Silver/Gold stages now
run Spark inside the `idp` image (JRE + pyspark + S3A jars baked in); the
`pandas|spark` engine switch, the `apache/spark` pods and the script ConfigMaps are
gone. After `make image && make app-deploy && make airflow-reload`, remove the
leftovers once:
```bash
kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow variables delete idp_silver_engine
kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow variables delete idp_gold_engine
kubectl -n idp-app delete configmap idp-silver-spark idp-gold-spark --ignore-not-found
```
Then verify with `make silver gold score publish` and `make timescale-verify`. The
Silver/Gold pods request 1.5 Gi / limit 3 Gi with a 1 g Spark driver heap
(`IDP_SPARK_DRIVER_MEMORY`); raise both together for bigger data. No lake
migration is needed — table names, layout and schemas are unchanged.

**Duplicate rows in Silver/Gold/labels after upgrading, or `publish` failing on
a UNIQUE index** (`UNIQUE constraint failed` / `could not create unique index
"ux_<table>_key"`): lakes written before idempotent stage writes stacked a new
copy of every Silver/Gold feature/labels table on each re-run. Stages now
*replace* the tenant partitions they write (Bronze and the score history still
append), and serving tables are uniquely keyed by `(tenant_id, service_id)`.
Clean an old lake once, with the new image built (`make image`):
```bash
# either start fresh ...
make minio-reset && make stream silver gold labels train score publish
# ... or keep Bronze and rewrite each derived table once (replace mode drops the copies)
make silver gold labels && make score publish
```
Run `make score` before `publish`: publish serves each tenant's latest score run,
and a run written before the upgrade may itself hold duplicates. Score history
(`credit_scores`/`fraud_scores`) keeps its earlier runs; duplicates inside an old
run only affect the 7/15/30-day MCP distributions.

`make platform-services` already applies the tuned Trino/Nessie values for fresh installs.
