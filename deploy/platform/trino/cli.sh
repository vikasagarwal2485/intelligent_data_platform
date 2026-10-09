#!/usr/bin/env bash
# Run one read-only SQL statement against the cluster's Trino as `admin`, from a
# short-lived pod of its own (used by `make lake-table-rows`).
#
# The Trino CLI is a JVM. Run inside the coordinator's container (kubectl exec), it
# shares that container's memory limit with the coordinator, and the two together
# can exceed it: the kernel then OOM-kills the whole coordinator (exit 137). Here
# the CLI gets its own pod and memory limit, and connects like any client: HTTPS to
# trino.idp-platform.svc:8443, trusting the certificate from the trino-tls Secret,
# with admin's password from the trino-passwords Secret (never on a command line).
#
# Usage:  bash deploy/platform/trino/cli.sh <namespace> <output-format> "<sql>"
set -euo pipefail
NS="${1:?namespace}"
FORMAT="${2:?output format}"
SQL="${3:?sql}"
POD="trino-cli-$(date +%s)-$$"

OVERRIDES=$(python3 - "${FORMAT}" "${SQL}" <<'EOF'
import json, sys
fmt, sql = sys.argv[1], sys.argv[2]
print(json.dumps({"spec": {
    "restartPolicy": "Never",
    "containers": [{
        "name": "cli",
        "image": "trinodb/trino:480",
        "imagePullPolicy": "IfNotPresent",
        "command": ["sh", "-c",
                    'exec java -Xmx192m -XX:+UseSerialGC -jar /usr/bin/trino '
                    '--server https://trino.idp-platform.svc:8443 '
                    '--truststore-path /tls/tls.crt --user admin --password '
                    '--output-format "$FORMAT" --execute "$TRINO_SQL"'],
        "env": [
            {"name": "FORMAT", "value": fmt},
            {"name": "TRINO_SQL", "value": sql},
            {"name": "TRINO_PASSWORD",
             "valueFrom": {"secretKeyRef": {"name": "trino-passwords", "key": "TRINO_ADMIN_PASSWORD"}}},
        ],
        "resources": {"requests": {"cpu": "100m", "memory": "256Mi"},
                      "limits": {"memory": "512Mi"}},
        "volumeMounts": [{"name": "tls", "mountPath": "/tls", "readOnly": True}],
    }],
    "volumes": [{"name": "tls", "secret": {"secretName": "trino-tls", "items": [{"key": "tls.crt", "path": "tls.crt"}]}}],
}}))
EOF
)

# --rm deletes the pod afterwards; --quiet keeps kubectl's own messages out of the
# output (the pod's stdout and stderr both arrive on stdout).
kubectl -n "${NS}" run "${POD}" --rm -i --quiet --restart=Never --image=trinodb/trino:480 \
  --overrides="${OVERRIDES}"
