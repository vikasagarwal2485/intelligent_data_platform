# Deployment

Kubernetes-native on both tiers (per the reviewed plan): **local = minikube**,
**production = AWS EKS**. The same Kustomize base is used by both overlays.

```
deploy/
  base/             # namespaces, RBAC, per-tenant isolation, scoring API
  overlays/local/   # minikube (1 replica, dev image)
  overlays/aws/     # EKS (3 replicas, ECR image, IRSA)
  argocd/           # GitOps Application for EKS
```

## What the base provides
- **Multi-tenancy & isolation** (`isolation.yaml`): per-tenant `ResourceQuota`
  and a default-deny `NetworkPolicy` that blocks cross-tenant pod traffic.
- **RBAC** (`rbac.yaml`): namespace-scoped roles (`ds-credit`, `ds-fraud`,
  `analyst`) bound to Keycloak OIDC group claims.
- **Scoring API** (`scoring-api.yaml`): the FastAPI credit/fraud service.

## Local (minikube)
```bash
make mk-up          # start minikube with ingress/metrics addons
make image          # build idp/scoring-api:dev INTO minikube's docker daemon
make deploy         # kubectl apply -k deploy/overlays/local
```

> **Why `make image` first?** The scoring API runs a project image that is not
> published to any registry. For local dev you build it directly into minikube's
> Docker daemon (`eval $(minikube docker-env) && docker build ...`) and the local
> overlay sets `imagePullPolicy: Never`. Skipping this causes the pod to try to
> pull `idp/scoring-api:dev` from Docker Hub and fail with `ImagePullBackOff`.
> On EKS the image instead comes from ECR (`overlays/aws`).

Platform infrastructure (Apache Kafka via Strimzi, Nessie + Iceberg, MinIO,
Trino, TimescaleDB, Keycloak, OPA, Airflow, MLflow, Prometheus/Grafana) is
installed by [`install-infra.sh`](install-infra.sh):

```bash
make infra        # or: bash deploy/install-infra.sh idp-platform
```

The script adds the required Helm repos before installing — omitting
`helm repo add` is what produces `Error: repo <x> not found`. It deliberately
**avoids Bitnami charts** (whose free images were removed on 2025-08-28, causing
`ImagePullBackOff` on e.g. `bitnami/minio:...`). Sources used:

| Component | Source | Image |
|-----------|--------|-------|
| MinIO | `https://charts.min.io/` (official) | `quay.io/minio/minio` |
| TimescaleDB | plain manifest `infra/timescaledb.yaml` | `timescale/timescaledb` |
| Keycloak | plain manifest `infra/keycloak.yaml` | `quay.io/keycloak/keycloak` |
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
kubectl apply -k deploy/overlays/aws        # or let Argo CD sync it
kubectl apply -f deploy/argocd/application.yaml
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
heap + worker pods, too big for a small node. Reinstall with the tuned values
(coordinator-only, ~1.5G heap):
```bash
helm upgrade --install trino trino/trino -n idp-platform -f deploy/trino-values.yaml
kubectl -n idp-platform rollout status deploy/trino-coordinator
```
`make infra` already applies this file for fresh installs.
