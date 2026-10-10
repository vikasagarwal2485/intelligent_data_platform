#!/usr/bin/env bash
# Provision the BI readers' tenant roles in the serving database (REQ-BI-03):
#   1. the timescaledb-bi Secret: IDP_BI_PASSWORD_<TENANT> per configured tenant,
#      random, created once; a new tenant's key is added, existing ones never change;
#   2. `idp bi-roles` in a short-lived pod (the platform image), as postgres:
#      the roles, grants, CONNECT revokes and row-level security on gold.*.
# Idempotent. Usage: bash deploy/platform/timescaledb/bi-roles.sh <namespace> <python>
set -euo pipefail
NS="${1:-idp-platform}"
PY="${2:-python3}"
TENANTS=$("${PY}" -c "from idp.common.config import load_config; print(' '.join(t.id for t in load_config().tenants))")

if ! kubectl -n "${NS}" get secret timescaledb-bi >/dev/null 2>&1; then
  kubectl -n "${NS}" create secret generic timescaledb-bi >/dev/null
fi
for t in ${TENANTS} meta; do                 # meta: the metadata-only identity (no data)
  key="IDP_BI_PASSWORD_$(printf '%s' "$t" | tr '[:lower:]' '[:upper:]')"
  if [ -z "$(kubectl -n "${NS}" get secret timescaledb-bi -o jsonpath="{.data.${key}}")" ]; then
    pw=$("${PY}" -c "import secrets; print(secrets.token_urlsafe(24))")
    kubectl -n "${NS}" patch secret timescaledb-bi --type merge \
      -p "{\"stringData\":{\"${key}\":\"${pw}\"}}" >/dev/null
    echo "timescaledb-bi: added ${key}"
  fi
done

OVERRIDES=$(cat <<JSON
{"spec": {"restartPolicy": "Never", "containers": [{
  "name": "bi-roles", "image": "idp/scoring-api:dev", "imagePullPolicy": "Never",
  "command": ["idp", "bi-roles"],
  "env": [
    {"name": "IDP_TS_HOST", "value": "timescaledb.${NS}.svc"},
    {"name": "IDP_TS_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "timescaledb", "key": "POSTGRES_PASSWORD"}}}],
  "envFrom": [{"secretRef": {"name": "timescaledb-bi"}}],
  "resources": {"limits": {"memory": "512Mi"}}}]}}
JSON
)
kubectl -n "${NS}" run "bi-roles-$(date +%s)" --rm -i --quiet --restart=Never \
  --image=idp/scoring-api:dev --overrides="${OVERRIDES}"
