#!/usr/bin/env bash
# Tune the Strimzi operator (probe delays + memory) THROUGH HELM, so Kubernetes
# field ownership stays with Helm. Do NOT use 'kubectl patch' for this: it makes
# 'kubectl-patch' the field owner and later 'helm upgrade' fails with
#   conflicts with "kubectl-patch" ... livenessProbe.initialDelaySeconds
#
# Symptom this addresses: operator Liveness/Readiness probe failures
# (context deadline exceeded / connection refused) on slow/over-subscribed nodes.
#
# Usage:  bash deploy/strimzi-tune.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

helm repo add strimzi https://strimzi.io/charts/ >/dev/null 2>&1 || true
helm repo update strimzi >/dev/null 2>&1 || true

echo ">> Tuning strimzi operator via Helm (memory + probe delays)"
if ! helm upgrade --install strimzi strimzi/strimzi-kafka-operator -n "${NS}" --reuse-values \
    --set resources.requests.cpu=200m --set resources.requests.memory=384Mi \
    --set resources.limits.cpu=1 --set resources.limits.memory=768Mi \
    --set livenessProbe.initialDelaySeconds=90 --set livenessProbe.timeoutSeconds=10 \
    --set readinessProbe.initialDelaySeconds=90 --set readinessProbe.timeoutSeconds=10; then
  cat >&2 <<EOF

Helm upgrade failed. This usually means the Deployment was previously modified
with 'kubectl patch' (field-ownership conflict). Do a CLEAN reinstall so Helm
owns the resource again:

  helm uninstall strimzi -n ${NS}
  helm upgrade --install strimzi strimzi/strimzi-kafka-operator -n ${NS} \\
    --set resources.limits.memory=768Mi \\
    --set livenessProbe.initialDelaySeconds=90 \\
    --set readinessProbe.initialDelaySeconds=90

(Helm does not delete CRDs on uninstall, so your Kafka CRDs are preserved.)
EOF
  exit 1
fi

kubectl -n "${NS}" rollout status deploy/strimzi-cluster-operator --timeout=300s
echo ">> Strimzi operator tuned and Ready."
