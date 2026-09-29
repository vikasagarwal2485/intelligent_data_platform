#!/usr/bin/env bash
# Install/upgrade the Spark Operator (pinned) + the Airflow RBAC to submit
# SparkApplications. Idempotent.  Usage: bash deploy/platform/spark-operator/install.sh [namespace]
set -euo pipefail
NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_VERSION="2.5.2"

helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update spark-operator >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator -n "${NS}" \
  --version "${CHART_VERSION}" -f "${HERE}/values.yaml" --wait --timeout 5m
kubectl apply -f "${HERE}/../airflow/rbac.yaml"
echo ">> Spark Operator ${CHART_VERSION} ready (watching idp-app, driver SA idp-spark)"
