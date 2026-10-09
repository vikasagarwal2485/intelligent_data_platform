#!/usr/bin/env bash
# Print a dev access token (REQ-SRV-03, REQ-TEN-01), fetched through a short-lived
# port-forward. Only the token goes to stdout:
#   TOKEN=$(bash deploy/platform/keycloak/token.sh tenant_a)            # = make -s keycloak-token TENANT=tenant_a
#   TOKEN=$(KIND=mcp bash deploy/platform/keycloak/token.sh tenant_b)   # = make -s keycloak-token TENANT=tenant_b KIND=mcp
#
# KIND selects the identity:
#   scoring (default)  client credentials of idp-<tenant>: a scoring-API caller
#   mcp                client credentials of idp-mcp-<tenant>: principal mcp@<tenant>,
#                      audience trino (the MCP server's Trino identity)
#   user               password grant of a dev user (DEV_USER, DEV_PASSWORD) through
#                      idp-dev-cli: the user's principal and tenant, audiences
#                      idp-scoring-api and trino
# Client secrets are the realm's dev defaults unless IDP_KC_SECRET_<CLIENT> is set
# (see src/idp/identity/realm.py). The token's issuer is KC_HOSTNAME
# (http://keycloak.idp-platform.svc:8080/realms/idp) however it is requested, so
# the in-cluster scoring API and Trino accept it.
set -euo pipefail
TENANT="${1:-tenant_a}"
NS="${2:-idp-platform}"
PORT="${KEYCLOAK_LOCAL_PORT:-18180}"
KIND="${KIND:-scoring}"
SLUG="${TENANT//_/-}"

secret_for() {   # IDP_KC_SECRET_<client id upper-cased, other characters as _>, else the default
  local var
  var="IDP_KC_SECRET_$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9]/_/g')"
  printf '%s' "${!var:-$2}"
}

case "${KIND}" in
  scoring) CLIENT="idp-${SLUG}";      FORM=(-d grant_type=client_credentials -d client_id="${CLIENT}" -d client_secret="$(secret_for "${CLIENT}" "${SLUG}-dev-secret")") ;;
  mcp)     CLIENT="idp-mcp-${TENANT}"; FORM=(-d grant_type=client_credentials -d client_id="${CLIENT}" -d client_secret="$(secret_for "${CLIENT}" "mcp-${SLUG}-dev-secret")") ;;
  user)    CLIENT="idp-dev-cli";       FORM=(-d grant_type=password -d client_id="${CLIENT}" -d username="${DEV_USER:?set DEV_USER}" -d password="${DEV_PASSWORD:-dev-password}") ;;
  *) echo "KIND must be scoring, mcp or user (got ${KIND})" >&2; exit 2 ;;
esac

if [ -n "${KEYCLOAK_URL:-}" ]; then          # an already reachable Keycloak (e.g. a local container)
  BASE="${KEYCLOAK_URL}"
else
  # A just-restarted Keycloak takes a minute or two to boot and import the realm.
  kubectl -n "${NS}" wait --for=condition=Ready pod -l app=keycloak --timeout=300s >/dev/null
  kubectl -n "${NS}" port-forward svc/keycloak "${PORT}:8080" >/dev/null 2>&1 &
  PF=$!
  trap 'kill ${PF} 2>/dev/null || true' EXIT
  BASE="http://127.0.0.1:${PORT}"
fi
for _ in $(seq 1 30); do
  curl -sf "${BASE}/realms/idp" >/dev/null 2>&1 && break
  sleep 1
done
RESP=$(curl -s -X POST "${BASE}/realms/idp/protocol/openid-connect/token" "${FORM[@]}")
TOKEN=$(printf '%s' "${RESP}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null || true)
if [ -z "${TOKEN}" ]; then
  echo "no ${KIND} token for ${TENANT} (client ${CLIENT}): ${RESP}" >&2
  exit 1
fi
printf '%s\n' "${TOKEN}"
