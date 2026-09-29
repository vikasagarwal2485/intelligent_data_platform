#!/usr/bin/env bash
# Print a dev access token for a tenant (REQ-SRV-03): client credentials of the
# realm's per-tenant client idp-<tenant> (dev secrets from idp-realm.json), fetched
# through a short-lived port-forward. Only the token goes to stdout:
#   TOKEN=$(bash deploy/platform/keycloak/token.sh tenant_a)     # = make -s keycloak-token TENANT=tenant_a
# The token's issuer is KC_HOSTNAME (http://keycloak.idp-platform.svc:8080/realms/idp)
# however it is requested, so the in-cluster scoring API accepts it.
set -euo pipefail
TENANT="${1:-tenant_a}"
NS="${2:-idp-platform}"
PORT="${KEYCLOAK_LOCAL_PORT:-18180}"
CLIENT="idp-${TENANT//_/-}"
SECRET="${TENANT//_/-}-dev-secret"

# A just-restarted Keycloak takes a minute or two to boot and import the realm.
kubectl -n "${NS}" wait --for=condition=Ready pod -l app=keycloak --timeout=300s >/dev/null
kubectl -n "${NS}" port-forward svc/keycloak "${PORT}:8080" >/dev/null 2>&1 &
PF=$!
trap 'kill ${PF} 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  curl -sf "http://127.0.0.1:${PORT}/realms/idp" >/dev/null 2>&1 && break
  sleep 1
done
RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/realms/idp/protocol/openid-connect/token" \
  -d grant_type=client_credentials -d client_id="${CLIENT}" -d client_secret="${SECRET}")
TOKEN=$(printf '%s' "${RESP}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null || true)
if [ -z "${TOKEN}" ]; then
  echo "no token for ${TENANT} (client ${CLIENT}): ${RESP}" >&2
  exit 1
fi
printf '%s\n' "${TOKEN}"
