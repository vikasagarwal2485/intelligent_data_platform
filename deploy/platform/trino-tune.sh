#!/usr/bin/env bash
# (Re)install Trino with the small-cluster values, auto-recovering from a stuck
# Helm release.
#
# 'Error: UPGRADE FAILED: another operation (install/upgrade/rollback) is in
# progress' means a previous helm op was interrupted and the release is stuck in
# a pending-* state (common when the cluster is slow and ops time out). Trino is
# stateless, so we clear the stuck release and reinstall cleanly.
#
# Usage:  bash deploy/platform/trino-tune.sh [namespace]
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

for s in trino-tls trino-auth trino-internal; do
  if ! kubectl -n "${NS}" get secret "${s}" >/dev/null 2>&1; then
    echo "ERROR: Secret ${s} is missing: run 'make trino-certs' first (Trino requires authentication)." >&2
    exit 1
  fi
done

echo ">> Installing/upgrading Trino with tuned values"
# The lake catalog and the access rules are shared with the compose test stack,
# so they are files, passed as values here.
helm upgrade --install trino trino/trino -n "${NS}" --version 1.42.2 \
  -f "${HERE}/trino/trino-values.yaml" \
  --set-file catalogs.lake="${HERE}/trino/lake.properties" \
  --set-file "accessControl.rules.rules\.json=${HERE}/trino/rules.json"
kubectl -n "${NS}" rollout status deploy/trino-coordinator --timeout=300s
echo ">> Trino coordinator is Ready."
