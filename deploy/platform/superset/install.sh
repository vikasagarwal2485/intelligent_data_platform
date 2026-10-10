#!/usr/bin/env bash
# Install or update Superset (REQ-PLT-12): the pinned Superset Kubernetes Operator,
# the superset-secrets Secret (created once), the Superset resource rendered from
# superset.yaml + superset_config.py, then `make bi-roles` again so the new
# `superset` metastore database is also closed to the tenant roles.
# Needs `make superset-image` (the tag is a hash of the image inputs), `make trino-certs`,
# `make bi-roles`. Usage: bash deploy/platform/superset/install.sh <namespace> <python> <operator-version>
set -euo pipefail
NS="${1:-idp-platform}"
PY="${2:-python3}"
OPERATOR_VERSION="${3:?operator chart version}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for s in trino-tls trino-passwords timescaledb timescaledb-bi; do
  kubectl -n "${NS}" get secret "${s}" >/dev/null 2>&1 || {
    echo "ERROR: Secret ${s} is missing (make trino-certs, make bi-roles)." >&2; exit 1; }
done
TAG=$("${PY}" "${HERE}/render.py" tag)
if ! minikube ssh -- docker image inspect "idp/superset:${TAG}" >/dev/null 2>&1; then
  echo "ERROR: image idp/superset:${TAG} is not in minikube: run 'make superset-image'." >&2; exit 1
fi

echo ">> Superset operator ${OPERATOR_VERSION} (watches ${NS} only)"
helm upgrade --install superset-operator \
  oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator \
  --version "${OPERATOR_VERSION}" -n "${NS}" -f "${HERE}/operator-values.yaml" --wait --timeout 5m >/dev/null

if ! kubectl -n "${NS}" get secret superset-secrets >/dev/null 2>&1; then
  echo ">> creating superset-secrets"
  kubectl -n "${NS}" create secret generic superset-secrets \
    --from-literal=SECRET_KEY="$("${PY}" -c 'import secrets; print(secrets.token_urlsafe(42))')" \
    --from-literal=IDP_OAUTH_CLIENT_SECRET="${IDP_KC_SECRET_IDP_SUPERSET:-superset-dev-secret}" \
    --from-literal=IDP_TRINO_SUPERSET_PASSWORD="$(kubectl -n "${NS}" get secret trino-passwords \
        -o jsonpath='{.data.TRINO_SUPERSET_PASSWORD}' | base64 -d)" >/dev/null
fi

echo ">> Superset resource (image idp/superset:${TAG})"
"${PY}" "${HERE}/render.py" resource | kubectl apply -f -
for _ in $(seq 1 60); do
  ready=$(kubectl -n "${NS}" get superset superset \
    -o jsonpath='{.status.conditions[?(@.type=="Available")].status}{.status.components.webServer.image}' 2>/dev/null || true)
  [ "${ready}" = "Trueidp/superset:${TAG}" ] && break
  sleep 10
done
[ "${ready}" = "Trueidp/superset:${TAG}" ] || { echo "ERROR: Superset not ready: kubectl -n ${NS} describe superset superset" >&2; exit 1; }
echo ">> Superset ready"
bash "${HERE}/../timescaledb/bi-roles.sh" "${NS}" "${PY}" >/dev/null
