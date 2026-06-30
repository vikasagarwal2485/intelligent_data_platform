#!/usr/bin/env bash
# Relax the Strimzi operator's liveness/readiness probes and raise its memory.
#
# Symptom this fixes:
#   Liveness/Readiness probe failed: Get ".../healthy|/ready":
#   context deadline exceeded / connection refused
# On a slow or over-subscribed node the operator can't answer the health
# endpoint within the tight default probe timeouts, so the kubelet kills it in a
# restart loop. We widen the timeouts/initialDelay and bump the memory limit.
#
# Usage:  bash deploy/strimzi-tune.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
DEP="strimzi-cluster-operator"

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

echo ">> Waiting for the ${DEP} deployment to exist..."
for i in $(seq 1 30); do
  kubectl -n "${NS}" get deploy "${DEP}" >/dev/null 2>&1 && break
  sleep 2
  [[ $i -eq 30 ]] && { echo "ERROR: ${DEP} not found (run 'make infra' first)." >&2; exit 1; }
done

echo ">> Relaxing probes and raising memory on ${DEP}"
kubectl -n "${NS}" patch deploy "${DEP}" -p '{
  "spec": {"template": {"spec": {"containers": [{
    "name": "strimzi-cluster-operator",
    "livenessProbe":  {"initialDelaySeconds": 90, "periodSeconds": 30, "timeoutSeconds": 10, "failureThreshold": 6},
    "readinessProbe": {"initialDelaySeconds": 90, "periodSeconds": 30, "timeoutSeconds": 10, "failureThreshold": 6},
    "resources": {"requests": {"cpu": "200m", "memory": "384Mi"}, "limits": {"cpu": "1", "memory": "768Mi"}}
  }]}}}
}'

echo ">> Waiting for rollout..."
if ! kubectl -n "${NS}" rollout status deploy/"${DEP}" --timeout=300s; then
  cat >&2 <<'EOF'

Operator still not Ready after tuning. This is almost certainly NODE RESOURCE
PRESSURE (the whole stack is heavy for a small minikube). Check:

  kubectl top nodes
  kubectl -n idp-platform describe pod -l strimzi.io/kind=cluster-operator | tail -30
  kubectl get events -n idp-platform --sort-by=.lastTimestamp | tail -20

Give minikube more headroom and retry:
  minikube stop && minikube start --cpus=8 --memory=16384
EOF
  exit 1
fi
echo ">> Strimzi operator is Ready."
