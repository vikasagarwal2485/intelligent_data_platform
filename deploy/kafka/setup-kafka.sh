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

# --- Preflight: the Strimzi CRDs must be installed by the operator ----------
# Applying a Kafka/KafkaNodePool before the operator's CRDs exist fails with
# 'no matches for kind "Kafka" in version "kafka.strimzi.io/v1beta2"'.
if ! kubectl get crd kafkas.kafka.strimzi.io >/dev/null 2>&1; then
  cat >&2 <<EOF
ERROR: Strimzi CRDs are not installed, so the Kafka cluster cannot be created.
The Strimzi *operator* (which installs the CRDs) must be running first.

Currently installed Strimzi CRDs:
$(kubectl get crd 2>/dev/null | grep -i strimzi || echo "  (none)")

Fix — install/repair the operator, then re-run 'make kafka':
  helm repo add strimzi https://strimzi.io/charts/ && helm repo update
  helm upgrade --install strimzi strimzi/strimzi-kafka-operator -n ${NS} --set resources.limits.memory=768Mi
  kubectl get crd | grep strimzi
  kubectl -n ${NS} rollout status deploy/strimzi-cluster-operator

If 'kubectl get crd | grep strimzi' shows nothing, install the operator+CRDs
from the official bundle instead:
  kubectl create -f 'https://strimzi.io/install/latest?namespace=${NS}' -n ${NS}
EOF
  exit 1
fi

# Surface the served API versions + node-pool support to catch mismatches.
SERVED="$(kubectl get crd kafkas.kafka.strimzi.io -o jsonpath='{.spec.versions[*].name}' 2>/dev/null || true)"
echo ">> Strimzi Kafka CRD served versions: ${SERVED:-unknown}"
case " ${SERVED} " in
  *" v1beta2 "*) : ;;
  *) echo "WARN: this manifest uses kafka.strimzi.io/v1beta2 but the CRD serves '${SERVED}'." >&2
     echo "      Edit the apiVersion in deploy/kafka/kafka-cluster.yaml to match." >&2 ;;
esac
if ! kubectl get crd kafkanodepools.kafka.strimzi.io >/dev/null 2>&1; then
  echo "WARN: KafkaNodePool CRD missing — your Strimzi version may predate node pools." >&2
  echo "      Upgrade Strimzi, or replace kafka-cluster.yaml with a ZooKeeper-based Kafka CR." >&2
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
