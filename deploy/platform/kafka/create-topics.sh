#!/usr/bin/env bash
# Create per-tenant Kafka topics as Strimzi KafkaTopic CRs (plan 3.1).
# The Strimzi Topic Operator reconciles these into real Kafka topics.
#
# Usage:  bash deploy/kafka/create-topics.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
CLUSTER="idp-kafka"

# Tenants (keep in sync with config/platform.yaml).
TENANTS=("tenant_a" "tenant_b")

# Message retention: Kafka deletes messages older than this automatically
# (cleanup.policy=delete). Default 7 days; override with RETENTION_DAYS=N.
# Re-run `make topics` after changing it — the Topic Operator reconciles the
# new retention onto the existing topics.
RETENTION_DAYS="${RETENTION_DAYS:-7}"
RETENTION_MS=$(( RETENTION_DAYS * 24 * 60 * 60 * 1000 ))

# Topic suffixes per the plan: entity (batch/CDC) + event (streaming) + DLQ.
SUFFIXES=(
  parties accounts agreements products
  usage.voice usage.data recharge fees dlq
)

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

# Detect the served API version of the KafkaTopic CRD (v1beta2 vs v1).
TVER="$(kubectl get crd kafkatopics.kafka.strimzi.io -o jsonpath='{.spec.versions[?(@.storage==true)].name}' 2>/dev/null || true)"
TVER="${TVER:-v1}"
echo ">> Creating KafkaTopics with apiVersion kafka.strimzi.io/${TVER}"

for t in "${TENANTS[@]}"; do
  for s in "${SUFFIXES[@]}"; do
    topic="t.${t}.${s}"
    # DNS-safe metadata name (no dots/underscores): t-tenant-a-usage-voice
    name="t-${t//_/-}-${s//./-}"
    partitions=3
    [[ "$s" == *usage* ]] && partitions=6   # higher throughput for usage events
    cat <<EOF | kubectl apply -n "${NS}" -f -
apiVersion: kafka.strimzi.io/${TVER}
kind: KafkaTopic
metadata:
  name: ${name}
  labels:
    strimzi.io/cluster: ${CLUSTER}
spec:
  topicName: ${topic}
  partitions: ${partitions}
  replicas: 1
  config:
    retention.ms: "${RETENTION_MS}"   # ${RETENTION_DAYS} days (plan section 2)
    cleanup.policy: delete
EOF
  done
done

echo ">> Topics applied. Verify with:"
echo "   kubectl -n ${NS} get kafkatopics"
echo "   kubectl -n ${NS} exec -it idp-kafka-dual-role-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
