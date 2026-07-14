#!/usr/bin/env bash
# (Re)install Trino with the small-cluster values, auto-recovering from a stuck
# Helm release.
#
# 'Error: UPGRADE FAILED: another operation (install/upgrade/rollback) is in
# progress' means a previous helm op was interrupted and the release is stuck in
# a pending-* state (common when the cluster is slow and ops time out). Trino is
# stateless, so we clear the stuck release and reinstall cleanly.
#
# Usage:  bash deploy/trino-tune.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

helm repo add trino https://trinodb.github.io/charts >/dev/null 2>&1 || true
helm repo update trino >/dev/null 2>&1 || true

# If the release is wedged in a pending state, clear the lock first.
if helm list -n "${NS}" --pending -q 2>/dev/null | grep -qx trino; then
  echo ">> trino release is stuck in a pending state — clearing it"
  # Prefer a rollback to the last completed revision; fall back to uninstall.
  if ! helm rollback trino -n "${NS}" >/dev/null 2>&1; then
    echo ">> no completed revision to roll back to; uninstalling"
    helm uninstall trino -n "${NS}" >/dev/null 2>&1 || true
  fi
fi

echo ">> Installing/upgrading Trino with tuned values"
helm upgrade --install trino trino/trino -n "${NS}" -f "${HERE}/trino/trino-values.yaml"
kubectl -n "${NS}" rollout status deploy/trino-coordinator --timeout=300s
echo ">> Trino coordinator is Ready."
