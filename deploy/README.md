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
installed from upstream operators/Helm charts into `idp-platform`. See the root
`README.md` for the full ordered bring-up.

## Production (AWS EKS)
```bash
kubectl apply -k deploy/overlays/aws        # or let Argo CD sync it
kubectl apply -f deploy/argocd/application.yaml
```
EKS specifics: S3 (per-tenant prefixes + KMS), IRSA for pod S3 access, External
Secrets for credentials, ECR for images, ALB ingress.
