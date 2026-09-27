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
    trino/  nessie/         #   per-service tuned Helm values
    airflow/                #   Airflow values, RBAC, DAG image Dockerfile
    monitoring/             #   Prometheus/Grafana values, dashboards, Kafka PodMonitor
    install-infra.sh        #   one-shot platform install (Helm repos + services)
    strimzi-tune.sh  trino-tune.sh  nessie-tune.sh   # self-healing repair scripts
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

## The Iceberg lake (JDBC catalog)

Bronze, Silver and Gold are Iceberg tables (ADR-0028), catalogued in an **Iceberg
JDBC catalog**: the `iceberg` database on the platform TimescaleDB/Postgres.
Every stage pod gets the catalog from `IDP_CATALOG_TYPE=sql` and
`IDP_CATALOG_URI=postgresql+psycopg2://…@timescaledb.idp-platform.svc:5432/iceberg`.

- **Catalog database.** `make lake-catalog` (also run by `install-infra.sh`)
  creates the `iceberg` database idempotently. The first stage creates the catalog
  tables. Tables live at `s3://idp-<layer>/tables/<table>`, and each layer
  namespace carries that location too.
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
make trino-fix        # coordinator-only, ~1G heap, auto-recovers stuck releases
```

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

**Strimzi entity-operator `OOMKilled` (exit 137)** (the `*-entity-operator` pod's
topic-operator/user-operator containers): the Kafka CR now gives the topic
operator explicit memory and drops the unused user operator — re-apply with
`make kafka`. This doesn't block anything: topics already exist and produce/consume
work while it restarts; the entity operator only reconciles `KafkaTopic` CRs.

**Keycloak `OOMKilled` (exit 137), CrashLoopBackOff**: the Quarkus JVM sized its
heap from the cgroup limit and total RSS exceeded the memory limit. The manifest
caps it with `JAVA_OPTS_APPEND=-XX:MaxRAMPercentage=50.0 -XX:MaxMetaspaceSize=256m`;
re-apply it (`kubectl apply -f deploy/platform/keycloak/keycloak.yaml`). Keycloak
is **optional** for the runnable prototype (RBAC design only) — on a memory-tight
node, `kubectl -n idp-platform scale deploy/keycloak --replicas=0` frees ~1.5Gi.

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
