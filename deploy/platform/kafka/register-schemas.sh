#!/usr/bin/env bash
# Register each Kafka topic's wire contract (REQ-STR-03) with Apicurio Registry
# via its Confluent-compatible API: the TM Forum create-event JSON Schema of the
# topic (`idp registry-schemas`), as schemaType JSON. Subjects follow the
# TopicNameStrategy: "<topic>-value", e.g. t.tenant_a.productUsage-value.
#
# The canonical Avro schemas in config/avro describe Bronze records, not the wire,
# so they aren't registered (REQ-STR-03).
#
# Usage:  bash deploy/kafka/register-schemas.sh [namespace]
set -euo pipefail

NS="${1:-idp-platform}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "${HERE}/../../.." && pwd)"                    # deploy/platform/kafka -> repo root
TENANTS=("tenant_a" "tenant_b")
# The repo's Python (with the idp package); override with PYTHON=...
PYTHON="${PYTHON:-${REPO}/.venv/bin/python}"
[[ -x "${PYTHON}" ]] || PYTHON="$(command -v python3 || true)"
[[ -n "${PYTHON}" ]] || { echo "python3 is required" >&2; exit 1; }

SCHEMA_DIR="$(mktemp -d)"
trap 'rm -rf "${SCHEMA_DIR}"' EXIT
( cd "${REPO}" && PYTHONPATH="${REPO}/src${PYTHONPATH:+:${PYTHONPATH}}" \
    "${PYTHON}" -m idp.cli registry-schemas --out "${SCHEMA_DIR}" ) \
  || { echo "ERROR: could not write the topic schemas (idp registry-schemas)" >&2; exit 1; }
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
trap 'kill ${PF} 2>/dev/null || true; rm -f "${PF_ERR}"; rm -rf "${SCHEMA_DIR}"' EXIT

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
  for file in "${SCHEMA_DIR}"/*.schema.json; do
    suffix="$(basename "${file}" .schema.json)"
    subject="t.${t}.${suffix}-value"
    body="$("${PYTHON}" -c "import json,sys; print(json.dumps({'schemaType':'JSON','schema':open(sys.argv[1]).read()}))" "${file}")"
    printf '>> %-48s <- %s ... ' "${subject}" "$(basename "${file}")"
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
