#!/usr/bin/env bash
# Install/upgrade the MLflow tracking server + model registry (REQ-ML-04), pinned,
# with its backend database `mlflow` on the platform TimescaleDB. Idempotent.
# Usage: bash deploy/platform/mlflow/install.sh [namespace]
set -euo pipefail
NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_VERSION="1.11.2"      # MLflow 3.14.0, = the mlflow-skinny client in requirements.txt

kubectl -n "${NS}" rollout status deploy/timescaledb --timeout=300s >/dev/null
until kubectl -n "${NS}" exec deploy/timescaledb -- pg_isready -U postgres >/dev/null 2>&1; do
  echo "waiting for timescaledb..."; sleep 3
done
if [ "$(kubectl -n "${NS}" exec deploy/timescaledb -- psql -U postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname = 'mlflow'")" != "1" ]; then
  kubectl -n "${NS}" exec deploy/timescaledb -- psql -U postgres -c "CREATE DATABASE mlflow"
fi

helm repo add community-charts https://community-charts.github.io/helm-charts >/dev/null 2>&1 || true
helm repo update community-charts >/dev/null
helm upgrade --install mlflow community-charts/mlflow -n "${NS}" \
  --version "${CHART_VERSION}" -f "${HERE}/mlflow-values.yaml" --wait --timeout 10m
echo ">> MLflow ${CHART_VERSION} ready: http://mlflow.${NS}.svc:80 (make mlflow-ui -> http://127.0.0.1:5050)"
