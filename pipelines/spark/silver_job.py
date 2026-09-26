"""Silver stage entrypoint for spark-submit (Bronze -> canonical Silver).

Thin wrapper over ``idp.pipeline.to_silver`` — the single Silver implementation
(``idp.transforms.silver``, REQ-ORC-05) shared with ``idp silver``, the Kubernetes
Job and Airflow. Requires the ``idp`` package in the Spark runtime (the idp image
bundles it). The lake location, tenant salts and Spark master/memory come from
config/platform.yaml + IDP_* env, exactly as for ``idp silver``.

  spark-submit pipelines/spark/silver_job.py [--config path/to/platform.yaml]
"""
from __future__ import annotations

import argparse
import json

from idp.common.config import load_config
from idp.pipeline import to_silver


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver stage (Bronze -> Silver)")
    parser.add_argument("--config", default=None, help="Path to platform.yaml")
    args = parser.parse_args()
    print(json.dumps(to_silver(load_config(args.config)), indent=2))


if __name__ == "__main__":
    main()
