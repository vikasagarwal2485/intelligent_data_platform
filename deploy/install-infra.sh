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
echo ">> Target namespace: ${NS}"
kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

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
hi minio minio/minio \
  --set mode=standalone \
  --set rootUser=minioadmin --set rootPassword=minioadmin123 \
  --set 'buckets[0].name=idp-bronze,buckets[0].policy=none' \
  --set 'buckets[1].name=idp-silver,buckets[1].policy=none' \
  --set 'buckets[2].name=idp-gold,buckets[2].policy=none' \
  --set 'buckets[3].name=idp-ml,buckets[3].policy=none' \
  --set resources.requests.memory=512Mi --set persistence.size=5Gi

echo ">> serving DB (TimescaleDB, upstream image, plain manifest)"
kubectl apply -n "${NS}" -f "${HERE}/infra/timescaledb.yaml"

echo ">> identity (Keycloak, upstream image, plain manifest)"
kubectl apply -n "${NS}" -f "${HERE}/infra/keycloak.yaml"

echo ">> streaming (Apache Kafka via Strimzi operator)"
hi strimzi strimzi/strimzi-kafka-operator
# After the operator is Ready, create a Kafka cluster CR (e.g. a Strimzi Kafka).

echo ">> Iceberg catalog (Nessie)"
hi nessie nessie/nessie

echo ">> query engine (Trino)"
hi trino trino/trino

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
