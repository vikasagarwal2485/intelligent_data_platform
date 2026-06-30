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

# Topic suffixes per the plan: entity (batch/CDC) + event (streaming) + DLQ.
SUFFIXES=(
  parties accounts agreements products
  usage.voice usage.data recharge dlq
)

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

for t in "${TENANTS[@]}"; do
  for s in "${SUFFIXES[@]}"; do
    topic="t.${t}.${s}"
    # DNS-safe metadata name (no dots/underscores): t-tenant-a-tmf-usage-voice
    name="t-${t//_/-}-${s//./-}"
    partitions=3
    [[ "$s" == *usage* ]] && partitions=6   # higher throughput for usage events
    cat <<EOF | kubectl apply -n "${NS}" -f -
apiVersion: kafka.strimzi.io/v1beta2
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
    retention.ms: "604800000"   # 7 days (plan section 2)
    cleanup.policy: delete
EOF
  done
done

echo ">> Topics applied. Verify with:"
echo "   kubectl -n ${NS} get kafkatopics"
echo "   kubectl -n ${NS} exec -it idp-kafka-dual-role-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
