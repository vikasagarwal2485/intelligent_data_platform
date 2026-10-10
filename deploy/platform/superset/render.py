#!/usr/bin/env python3
"""Render the `Superset` resource for the operator (REQ-PLT-12).

Prints superset.yaml with:
- ``spec.config``: superset_config.py, appended by the operator after its own
  generated settings;
- ``spec.image.tag``: a hash of the image inputs (Dockerfile, idp_superset.py). The
  operator runs ``superset db upgrade`` only when the tag string changes, so a mutable
  tag would skip migrations after a rebuild.

Usage: render.py tag         -> the image tag (for `make superset-image`)
       render.py resource    -> the resource YAML (for `kubectl apply -f -`)
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
IMAGE_INPUTS = ("Dockerfile", "idp_superset.py")
TENANTS_FROM = HERE.parents[2] / "config" / "platform.yaml"


def image_tag() -> str:
    h = hashlib.sha256()
    for name in IMAGE_INPUTS:
        h.update(name.encode() + b"\0" + (HERE / name).read_bytes())
    return h.hexdigest()[:12]


def tenants() -> list[str]:
    return [t["id"] for t in yaml.safe_load(TENANTS_FROM.read_text())["tenants"]]


def resource() -> dict:
    r = yaml.safe_load((HERE / "superset.yaml").read_text())
    r["spec"]["image"]["tag"] = image_tag()
    r["spec"]["config"] = (HERE / "superset_config.py").read_text()
    env = r["spec"]["podTemplate"]["container"].setdefault("env", [])
    env.append({"name": "IDP_TENANTS", "value": ",".join(tenants())})
    return r


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "resource"
    if what == "tag":
        print(image_tag())
    else:
        print(yaml.safe_dump(resource(), sort_keys=False, width=1000), end="")
