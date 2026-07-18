#!/usr/bin/env bash
# On-demand purge: delete ALL messages from the IDP Kafka topics *now*, without
# deleting the topics themselves. Uses kafka-delete-records with offset -1 (the
# high watermark), so every currently-committed record is truncated.
#
# This is the manual/immediate counterpart to time-based retention: topics are
# already configured with retention.ms=7d (create-topics.sh), which expires old
# messages automatically; this script empties them on demand.
#
# Usage:
#   bash deploy/platform/kafka/purge-topics.sh [namespace] [topic]
#     namespace  Kafka namespace (default: idp-platform)
#     topic      a single topic, e.g. t.tenant_a.usage.voice (default: all t.* topics)
#
# Examples:
#   bash deploy/platform/kafka/purge-topics.sh                       # purge all t.* topics
#   bash deploy/platform/kafka/purge-topics.sh idp-platform t.tenant_a.recharge
set -euo pipefail

NS="${1:-idp-platform}"
ONLY_TOPIC="${2:-}"
CLUSTER="idp-kafka"

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

POD="$(kubectl -n "${NS}" get pods \
        -l "strimzi.io/cluster=${CLUSTER},strimzi.io/kind=Kafka" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -z "${POD}" ]]; then
  echo "ERROR: no running Kafka broker pod for cluster '${CLUSTER}' in namespace '${NS}'." >&2
  echo "       Is Kafka deployed?  kubectl -n ${NS} get kafka,pods -l strimzi.io/cluster=${CLUSTER}" >&2
  exit 1
fi

echo ">> Purging messages via broker pod ${POD} (namespace ${NS})"
[[ -n "${ONLY_TOPIC}" ]] && echo ">> Target topic: ${ONLY_TOPIC}" || echo ">> Target: all topics matching '^t\\.'"

# Run inside the broker pod (has bin/kafka-*.sh, WORKDIR=/opt/kafka). Build an
# offset-json file (offset -1 per partition = delete up to the log end) per topic.
kubectl -n "${NS}" exec "${POD}" -- env ONLY_TOPIC="${ONLY_TOPIC}" bash -c '
set -euo pipefail
BS=localhost:9092
if [ -n "${ONLY_TOPIC}" ]; then
  TOPICS="${ONLY_TOPIC}"
else
  TOPICS=$(bin/kafka-topics.sh --bootstrap-server "$BS" --list | grep -E "^t\." || true)
fi
if [ -z "${TOPICS}" ]; then
  echo "   (no matching topics to purge)"; exit 0
fi
for t in ${TOPICS}; do
  pc=$(bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic "$t" \
        | sed -n "s/.*PartitionCount:[[:space:]]*\([0-9][0-9]*\).*/\1/p" | head -1)
  if [ -z "${pc}" ]; then
    echo "   ! ${t}: could not read partition count, skipping"; continue
  fi
  parts=""; p=0
  while [ "$p" -lt "$pc" ]; do
    parts="${parts}{\"topic\":\"$t\",\"partition\":$p,\"offset\":-1},"
    p=$((p+1))
  done
  printf "{\"partitions\":[%s],\"version\":1}" "${parts%,}" > /tmp/purge.json
  echo ">> purging ${t} (${pc} partitions)"
  bin/kafka-delete-records.sh --bootstrap-server "$BS" --offset-json-file /tmp/purge.json \
    | tail -n +1 | grep -E "partition|offset" | head -"$pc" || true
done
echo ">> Purge complete."
'
