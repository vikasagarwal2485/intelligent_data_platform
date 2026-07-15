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
    jobs/                   #   produce/consume/silver/gold/publish/spark-gold Jobs
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
make app-deploy     # kubectl apply -k deploy/app/overlays/local       (scoring-api)
```

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

`make platform-services` already applies the tuned Trino/Nessie values for fresh installs.
