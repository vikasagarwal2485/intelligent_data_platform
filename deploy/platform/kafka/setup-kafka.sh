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

if ! kubectl get crd kafkanodepools.kafka.strimzi.io >/dev/null 2>&1; then
  echo "WARN: KafkaNodePool CRD missing — your Strimzi version may predate node pools." >&2
  echo "      Upgrade Strimzi, or replace kafka-cluster.yaml with a ZooKeeper-based Kafka CR." >&2
fi

# Detect the API version the CRD actually serves (Strimzi promoted the group
# from v1beta2 to v1) and render the manifest with it, so this works on either.
KVER="$(kubectl get crd kafkas.kafka.strimzi.io -o jsonpath='{.spec.versions[?(@.storage==true)].name}' 2>/dev/null || true)"
KVER="${KVER:-v1}"

# Auto-select a Kafka broker version this operator actually supports (the list
# changes between Strimzi releases). We read the operator's STRIMZI_KAFKA_IMAGES
# env (entries like "4.3.0=quay.io/strimzi/kafka:...") and pick the latest.
KAFKAVER="$(kubectl -n "${NS}" get deploy strimzi-cluster-operator \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="STRIMZI_KAFKA_IMAGES")].value}' 2>/dev/null \
  | tr ' ' '\n' | sed -n 's/^\([0-9][0-9.]*\)=.*/\1/p' | sort -V | tail -1)"
echo ">> Using API kafka.strimzi.io/${KVER}; Kafka version ${KAFKAVER:-<manifest default>}"

echo "==> 1/5 Deploy Kafka cluster + Schema Registry"
# The Kafka CR references the kafka-metrics ConfigMap (JMX exporter rules for the
# produce/consume dashboard). Apply it first so the cluster is Ready (REQ-PLT-08).
kubectl apply -n "${NS}" -f "${HERE}/../monitoring/kafka-metrics-configmap.yaml"
# Render the manifest: substitute the served API version, and the detected Kafka
# version if we found one (otherwise keep the manifest's default).
sed -E "s#(kafka\.strimzi\.io/)v[0-9a-z]+#\1${KVER}#g" "${HERE}/kafka-cluster.yaml" \
  | if [ -n "${KAFKAVER}" ]; then sed -E "s/^( *version: ).*/\1${KAFKAVER}/"; else cat; fi \
  | kubectl apply -n "${NS}" -f -
kubectl apply -n "${NS}" -f "${HERE}/schema-registry.yaml"

echo "==> 2/5 Wait for Kafka to become Ready (this can take a few minutes)"
if ! kubectl -n "${NS}" wait kafka/idp-kafka --for=condition=Ready --timeout=600s; then
  echo "" >&2
  echo "ERROR: Kafka did not become Ready. Diagnostics:" >&2
  echo "--- Kafka status conditions ---" >&2
  kubectl -n "${NS}" get kafka idp-kafka \
    -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.reason}: {.message}{"\n"}{end}' 2>/dev/null >&2 || true
  echo "--- pods (strimzi.io/cluster=idp-kafka) ---" >&2
  kubectl -n "${NS}" get pods -l strimzi.io/cluster=idp-kafka >&2 || true
  echo "--- recent pod events ---" >&2
  kubectl -n "${NS}" describe pod -l strimzi.io/cluster=idp-kafka 2>/dev/null | grep -A20 -i "events:" | tail -20 >&2 || true
  cat >&2 <<EOF
Common causes:
  * Unsupported spec.kafka.version: edit deploy/platform/kafka/kafka-cluster.yaml to a
    version your operator supports (the status message above names it), e.g. 3.8.0.
  * Pod Pending / OOM: node is out of resources — give minikube more headroom
    (minikube stop && minikube start --cpus=8 --memory=16384) or free other pods.
EOF
  exit 1
fi

echo "==> 3/5 Create topics"
bash "${HERE}/create-topics.sh" "${NS}"

echo "==> 4/5 Wait for Schema Registry"
kubectl -n "${NS}" rollout status deploy/apicurio-registry --timeout=300s

echo "==> 5/5 Register Avro schemas"
bash "${HERE}/register-schemas.sh" "${NS}"

echo "==> Done."
