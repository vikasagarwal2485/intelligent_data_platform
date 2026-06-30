#!/usr/bin/env bash
# One-shot: deploy the Kafka cluster + Schema Registry, then create topics and
# register Avro schemas (plan section 3). Run AFTER `make infra` (which installs
# the Strimzi operator).
#
# Usage:  bash deploy/kafka/setup-kafka.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

echo "==> 1/5 Deploy Kafka cluster + Schema Registry"
kubectl apply -n "${NS}" -f "${HERE}/kafka-cluster.yaml"
kubectl apply -n "${NS}" -f "${HERE}/schema-registry.yaml"

echo "==> 2/5 Wait for Kafka to become Ready (this can take a few minutes)"
kubectl -n "${NS}" wait kafka/idp-kafka --for=condition=Ready --timeout=600s

echo "==> 3/5 Create topics"
bash "${HERE}/create-topics.sh" "${NS}"

echo "==> 4/5 Wait for Schema Registry"
kubectl -n "${NS}" rollout status deploy/apicurio-registry --timeout=300s

echo "==> 5/5 Register Avro schemas"
bash "${HERE}/register-schemas.sh" "${NS}"

echo "==> Done."
