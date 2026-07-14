#!/usr/bin/env bash
# Register Avro schemas (config/avro/*.avsc) with Apicurio Registry via its
# Confluent-compatible API. Subjects follow the TopicNameStrategy:
# "<topic>-value" (plan section 3.3).
#
# Usage:  bash deploy/kafka/register-schemas.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AVRO_DIR="$(cd "${HERE}/../../../config/avro" && pwd)"   # deploy/platform/kafka -> repo root
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

# Port-forward to the registry (ClusterIP) and clean up on exit. Use an uncommon
# LOCAL port: 8080 is frequently already bound on dev machines (Trino, Airflow
# UI, Kafka UI all use it), which makes port-forward fail to bind and every
# readiness probe miss. Override with REGISTRY_LOCAL_PORT if 18080 is taken too.
LPORT="${REGISTRY_LOCAL_PORT:-18080}"
echo ">> Port-forwarding apicurio-registry ${LPORT}->8080..."
PF_ERR="$(mktemp)"
kubectl -n "${NS}" port-forward svc/apicurio-registry "${LPORT}:8080" >/dev/null 2>"${PF_ERR}" &
PF=$!
trap 'kill ${PF} 2>/dev/null || true; rm -f "${PF_ERR}"' EXIT

ready=""
for i in $(seq 1 30); do
  if ! kill -0 "${PF}" 2>/dev/null; then
    echo "ERROR: port-forward exited early:" >&2; cat "${PF_ERR}" >&2
    echo "Hint: local port ${LPORT} may be in use — free it, close other" >&2
    echo "      port-forwards, or re-run with REGISTRY_LOCAL_PORT=<free-port>." >&2
    exit 1
  fi
  curl -sf "http://localhost:${LPORT}/health/ready" >/dev/null 2>&1 && { ready=1; break; }
  sleep 1
done
[[ -n "${ready}" ]] || { echo "registry not reachable on localhost:${LPORT}" >&2; cat "${PF_ERR}" >&2; exit 1; }

REG="http://localhost:${LPORT}/apis/ccompat/v7"

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
