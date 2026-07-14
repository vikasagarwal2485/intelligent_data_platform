#!/usr/bin/env bash
# Register Avro schemas (config/avro/*.avsc) with Apicurio Registry via its
# Confluent-compatible API. Subjects follow the TopicNameStrategy:
# "<topic>-value" (plan section 3.3).
#
# Usage:  bash deploy/kafka/register-schemas.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AVRO_DIR="$(cd "${HERE}/../../config/avro" && pwd)"
TENANTS=("tenant_a" "tenant_b")

# Map each streaming topic suffix -> the Avro schema file that defines its value.
# (Entity topics are batch/CDC; add their schemas here the same way if needed.)
MAP=(
  "usage.voice:usage_event.avsc"
  "usage.data:usage_event.avsc"
  "recharge:recharge_event.avsc"
)

command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: Kubernetes API unreachable. Start minikube and retry." >&2
  exit 1
fi

# Port-forward to the registry (ClusterIP) and clean up on exit.
echo ">> Port-forwarding apicurio-registry 8080..."
kubectl -n "${NS}" port-forward svc/apicurio-registry 8080:8080 >/dev/null 2>&1 &
PF=$!
trap 'kill ${PF} 2>/dev/null || true' EXIT

for i in $(seq 1 30); do
  curl -sf http://localhost:8080/health/ready >/dev/null 2>&1 && break
  sleep 1
  [[ $i -eq 30 ]] && { echo "registry not reachable" >&2; exit 1; }
done

REG="http://localhost:8080/apis/ccompat/v7"

for t in "${TENANTS[@]}"; do
  for pair in "${MAP[@]}"; do
    suffix="${pair%%:*}"; file="${pair##*:}"
    subject="t.${t}.${suffix}-value"
    body="$(python3 -c "import json,sys; print(json.dumps({'schemaType':'AVRO','schema':open(sys.argv[1]).read()}))" "${AVRO_DIR}/${file}")"
    printf '>> %-40s <- %s ... ' "${subject}" "${file}"
    if curl -sf -X POST "${REG}/subjects/${subject}/versions" \
         -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
         -d "${body}" >/dev/null; then
      echo "OK"
    else
      echo "FAILED"; exit 1
    fi
  done
done

echo ">> Done. List subjects with:  curl -s ${REG}/subjects"
