#!/usr/bin/env bash
# Install the platform infrastructure into the idp-platform namespace (plan 2A).
#
# Adds the required Helm repositories first (this is what was missing when you
# hit "repo <x> not found"), then installs each component.
#
# IMPORTANT: this script intentionally AVOIDS Bitnami charts. Since 2025-08-28
# Bitnami's free image catalogue is limited and tags like
# bitnami/minio:2025.7.23-debian-12-r3 were removed, causing ImagePullBackOff.
# MinIO uses its official chart; TimescaleDB and Keycloak use plain manifests
# with upstream images (timescale/timescaledb, quay.io/keycloak).
#
# Usage:  bash deploy/install-infra.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Preflight: the Kubernetes API must be reachable -------------------------
echo ">> Checking cluster connectivity (context: $(kubectl config current-context 2>/dev/null || echo none))"
if ! kubectl cluster-info >/dev/null 2>&1; then
  cat >&2 <<'EOF'
ERROR: cannot reach the Kubernetes API server.

This is a cluster/kubeconfig problem, not a chart problem (you saw a
"TLS handshake timeout" / "failed to download openapi"). Fix the cluster first:

  minikube status            # is it Running?
  minikube start             # start it if Stopped/Paused
  minikube update-context    # fix a stale kubeconfig port after a restart
  kubectl cluster-info       # should print the control-plane URL

If you use Docker Desktop, make sure it is running and not paused.
Then re-run:  make infra
EOF
  exit 1
fi

echo ">> Target namespace: ${NS}"
kubectl create namespace "${NS}" 2>/dev/null || true

echo ">> Adding Helm repositories"
helm repo add minio                https://charts.min.io/
helm repo add strimzi              https://strimzi.io/charts/
helm repo add nessie               https://charts.projectnessie.org
helm repo add trino                https://trinodb.github.io/charts
helm repo add apache-airflow       https://airflow.apache.org
helm repo add community-charts     https://community-charts.github.io/helm-charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Remove any earlier broken Bitnami releases (safe if absent).
for r in minio timescaledb keycloak postgresql; do
  helm uninstall "$r" -n "${NS}" >/dev/null 2>&1 || true
done

hi() { helm upgrade --install "$@" --namespace "${NS}"; }

echo ">> object store (MinIO, official chart, image quay.io/minio/minio)"
# Pin to the last release with a fully functional community Console. MinIO
# stripped bucket/object/admin management out of the community web console in
# RELEASE.2025-05-24 onward (object-browser-only, effectively read-only), so the
# `latest` image gives an unusable "can't click anything" console. See
# https://github.com/minio/minio/discussions/21326
MINIO_TAG="RELEASE.2025-04-22T22-12-26Z"
hi minio minio/minio \
  --set mode=standalone \
  --set image.repository=quay.io/minio/minio --set image.tag="${MINIO_TAG}" \
  --set rootUser=minioadmin --set rootPassword=minioadmin123 \
  --set 'buckets[0].name=idp-bronze,buckets[0].policy=none' \
  --set 'buckets[1].name=idp-silver,buckets[1].policy=none' \
  --set 'buckets[2].name=idp-gold,buckets[2].policy=none' \
  --set 'buckets[3].name=idp-ml,buckets[3].policy=none' \
  --set resources.requests.memory=512Mi --set persistence.size=5Gi

echo ">> serving DB (TimescaleDB, upstream image, plain manifest)"
kubectl apply -n "${NS}" -f "${HERE}/timescaledb/timescaledb.yaml"

echo ">> identity (Keycloak, upstream image, plain manifest)"
kubectl apply -n "${NS}" -f "${HERE}/keycloak/keycloak.yaml"

echo ">> streaming (Apache Kafka via Strimzi operator)"
# Tune resources + probe delays via Helm values (single field owner = Helm).
# Do NOT 'kubectl patch' the operator afterwards or later helm upgrades will
# fail with a field-ownership conflict.
hi strimzi strimzi/strimzi-kafka-operator \
  --set resources.requests.cpu=200m --set resources.requests.memory=384Mi \
  --set resources.limits.cpu=1 --set resources.limits.memory=768Mi \
  --set livenessProbe.initialDelaySeconds=90 --set livenessProbe.timeoutSeconds=10 \
  --set readinessProbe.initialDelaySeconds=90 --set readinessProbe.timeoutSeconds=10
echo ">> Verifying Strimzi CRDs were installed:"
kubectl get crd 2>/dev/null | grep -i strimzi || echo "  WARN: no Strimzi CRDs found yet"
# After the operator is Ready, run: make kafka  (creates the Kafka cluster CR).

echo ">> Iceberg catalog (Nessie, in-memory store, tuned)"
hi nessie nessie/nessie -f "${HERE}/nessie/nessie-values.yaml"

echo ">> query engine (Trino, tuned small for local)"
hi trino trino/trino -f "${HERE}/trino/trino-values.yaml"

echo ">> orchestration (Airflow)"
hi airflow apache-airflow/airflow

echo ">> experiment tracking (MLflow)"
hi mlflow community-charts/mlflow

echo ">> observability (Prometheus + Grafana)"
hi kube-prom prometheus-community/kube-prometheus-stack

cat <<EONOTE

>> Done. Watch rollout with:  kubectl get pods -n ${NS} -w

All components use freely-pullable upstream images (quay.io/minio,
timescale/timescaledb, quay.io/keycloak, trinodb/trino, projectnessie,
apache/airflow, quay.io prometheus) - no Bitnami images required.
EONOTE
