#!/usr/bin/env bash
# (Re)install Nessie with the small-cluster values, auto-recovering a stuck
# Helm release (same pattern as trino-tune.sh).
#
# Usage:  bash deploy/nessie-tune.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

helm repo add nessie https://charts.projectnessie.org >/dev/null 2>&1 || true
helm repo update nessie >/dev/null 2>&1 || true

if helm list -n "${NS}" --pending -q 2>/dev/null | grep -qx nessie; then
  echo ">> nessie release stuck in a pending state — clearing it"
  helm rollback nessie -n "${NS}" >/dev/null 2>&1 || helm uninstall nessie -n "${NS}" >/dev/null 2>&1 || true
fi

echo ">> Installing/upgrading Nessie with tuned values (in-memory store)"
helm upgrade --install nessie nessie/nessie -n "${NS}" -f "${HERE}/nessie-values.yaml"
kubectl -n "${NS}" rollout status deploy/nessie --timeout=300s
echo ">> Nessie is Ready."
