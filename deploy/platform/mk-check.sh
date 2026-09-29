#!/usr/bin/env bash
# Node headroom check (REQ-ORC-07): with the Docker driver the minikube node is a
# container with its own memory cap, which Kubernetes doesn't see. If the cap can't
# hold the platform plus the largest Spark stage (driver + executors, heap +
# overhead), the kernel's OOM killer takes the biggest process (a Spark JVM) long
# before any pod reaches its own limit. This checks that before you run a stage.
#
# Usage:  bash deploy/platform/mk-check.sh     (make mk-check)
#         IDP_CONFIG=<platform.yaml> to check other profiles
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PY="${HERE}/.venv/bin/python"; [ -x "${PY}" ] || PY=python3
mib() { echo $(( $1 / 1024 / 1024 )); }

cap=$(docker inspect minikube --format '{{.HostConfig.Memory}}' 2>/dev/null || echo "")
[ -n "${cap}" ] || { echo "mk-check: no 'minikube' Docker container (not the docker driver?) - nothing to check"; exit 0; }
node_kib=$(kubectl get node minikube -o jsonpath='{.status.capacity.memory}' | tr -d 'Ki')
node=$(( node_kib * 1024 ))
[ "${cap}" != "0" ] || cap=${node}                         # 0 = no cap: the VM's memory applies

# what is running now (platform + apps), from metrics-server
top=$(kubectl top pods -A --no-headers 2>/dev/null || true)
running=$(kubectl get pods -A --no-headers --field-selector=status.phase=Running 2>/dev/null | wc -l | tr -d ' ')
measured=$(printf '%s' "${top}" | grep -c . || true)
# a just-restarted metrics-server reports only a few pods: treat that as unavailable
if [ -z "${top}" ] || [ "${measured}" -lt $(( running * 8 / 10 )) ]; then
  echo "warning: metrics-server unavailable or incomplete (${measured:-0} of ${running} running pods measured) -" >&2
  echo "         checking the stage profile alone," >&2
  echo "         without the platform's current usage; re-run once 'kubectl top pods -A' works" >&2
  used=0
else
  used=$(echo "${top}" | awk '{v=$4; if (v ~ /Gi$/) {sub(/Gi$/,"",v); s+=v*1024} else if (v ~ /Mi$/) {sub(/Mi$/,"",v); s+=v} else if (v ~ /Ki$/) {sub(/Ki$/,"",v); s+=v/1024}} END {printf "%d", s}')
fi

read -r stage need <<<"$("${PY}" - <<'PY'
import os
from idp.common.config import load_config
from idp.transforms.session import stage_memory_mib
cfg = load_config(os.environ.get("IDP_CONFIG") or None)
stages = cfg.raw.get("spark", {}).get("profiles", {})
best = max(((s, stage_memory_mib(cfg, s)["total"]) for s in stages), key=lambda x: x[1], default=("-", 0))
print(best[0], best[1])
PY
)"

margin=512                                                  # kubelet, containerd, page cache
want=$(( used + need + margin ))
echo "minikube node container cap : $(mib "${cap}") MiB   (Kubernetes reports $(mib "${node}") MiB)"
echo "running pods now            : ${used} MiB"
echo "largest Spark stage         : ${need} MiB  (${stage}: driver + executors, heap + overhead)"
echo "needed                      : ${want} MiB  (incl. ${margin} MiB margin)"
if [ "$(mib "${cap}")" -lt "${want}" ]; then
  echo "FAIL: the node container can't hold the platform plus a ${stage} run; Spark pods would be" >&2
  echo "      OOM-killed by the node, below their own limits. Raise the cap (live, keeps state):" >&2
  echo "        docker update --memory $(( (want / 1024) + 2 ))g --memory-swap $(( (want / 1024) + 2 ))g minikube" >&2
  exit 1
fi
if [ "${cap}" -lt "${node}" ]; then
  echo "note: Kubernetes sees $(mib "${node}") MiB but the container is capped at $(mib "${cap}") MiB;"
  echo "      schedulable memory is overstated, so keep to one Spark stage at a time."
fi
echo "OK"
