#!/usr/bin/env bash
# Create per-tenant Kafka topics as Strimzi KafkaTopic CRs.
# The Strimzi Topic Operator reconciles these into real Kafka topics.
#
# Usage:  bash deploy/platform/kafka/create-topics.sh [namespace]
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

# Topic suffixes: one per TM Forum resource, each carrying its create events
# (REQ-STR-01; keep in sync with idp.streaming.TOPIC_RESOURCES), plus the DLQ.
# Individuals and organizations share `party`; voice and data share `productUsage`.
# Every topic uses the delete policy with RETENTION_DAYS: Bronze keeps every event,
# and Silver resolves customers from it (REQ-SLV-09), so nothing is log-compacted.
SUFFIXES=(
  customer party billingAccount agreement product
  appliedCustomerBillingRate topupBalance productUsage dlq
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
    # DNS-safe metadata name (lower case, no dots/underscores): t-tenant-a-productusage
    # (`tr`, not ${s,,}: macOS ships bash 3.2)
    name="$(printf 't-%s-%s' "${t//_/-}" "${s//./-}" | tr '[:upper:]' '[:lower:]')"
    partitions=3
    [[ "$s" == productUsage ]] && partitions=6   # higher throughput for usage events
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
    retention.ms: "${RETENTION_MS}"   # ${RETENTION_DAYS} days
    cleanup.policy: delete
EOF
  done
done

echo ">> Topics applied. Verify with:"
echo "   kubectl -n ${NS} get kafkatopics"
echo "   kubectl -n ${NS} exec -it idp-kafka-dual-role-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
