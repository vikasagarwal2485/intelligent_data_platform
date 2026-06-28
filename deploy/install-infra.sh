#!/usr/bin/env bash
# Install the platform infrastructure into the idp-platform namespace (plan 2A).
#
# Adds the required Helm repositories first (this is what was missing when you
# hit "repo <x> not found"), then installs each component. Bitnami charts are
# pulled via their OCI registry, so they need no `helm repo add`.
#
# Usage:  bash deploy/install-infra.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
echo ">> Target namespace: ${NS}"
kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

echo ">> Adding Helm repositories"
helm repo add strimzi              https://strimzi.io/charts/
helm repo add nessie               https://charts.projectnessie.org
helm repo add trino                https://trinodb.github.io/charts
helm repo add apache-airflow       https://airflow.apache.org
helm repo add community-charts     https://community-charts.github.io/helm-charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Helper: install-or-upgrade so the script is idempotent (safe to re-run).
hi() { helm upgrade --install "$@" --namespace "${NS}"; }

echo ">> object store (MinIO, Bitnami OCI)"
hi minio       oci://registry-1.docker.io/bitnamicharts/minio

echo ">> streaming (Apache Kafka via Strimzi operator)"
hi strimzi     strimzi/strimzi-kafka-operator
# After the operator is Ready, create a Kafka cluster:
#   kubectl apply -f deploy/kafka/kafka-cluster.yaml -n ${NS}

echo ">> Iceberg catalog (Nessie)"
hi nessie      nessie/nessie

echo ">> query engine (Trino)"
hi trino       trino/trino

echo ">> serving DB (PostgreSQL/TimescaleDB, Bitnami OCI)"
hi timescaledb oci://registry-1.docker.io/bitnamicharts/postgresql

echo ">> identity (Keycloak, Bitnami OCI)"
hi keycloak    oci://registry-1.docker.io/bitnamicharts/keycloak

echo ">> orchestration (Airflow)"
hi airflow     apache-airflow/airflow

echo ">> experiment tracking (MLflow)"
hi mlflow      community-charts/mlflow

echo ">> observability (Prometheus + Grafana)"
hi kube-prom   prometheus-community/kube-prometheus-stack

cat <<EONOTE

>> Done. Check rollout with:  kubectl get pods -n ${NS} -w

NOTE on Bitnami charts (minio / postgresql / keycloak):
Since 2025-08-28 Bitnami serves only a limited free image catalogue; older
free tags moved to the 'bitnamilegacy' repo. If a Bitnami pod fails to pull
its image, re-run that chart pinning a legacy image, e.g.:

  helm upgrade --install timescaledb oci://registry-1.docker.io/bitnamicharts/postgresql \\
    --namespace ${NS} \\
    --set image.registry=docker.io \\
    --set image.repository=bitnamilegacy/postgresql \\
    --set global.security.allowInsecureImages=true

Or substitute a non-Bitnami chart (e.g. CloudNativePG for Postgres,
codecentric/keycloakx for Keycloak).
EONOTE
