"""Gold stage entrypoint for spark-submit (Silver -> Gold feature tables).

Thin wrapper over ``idp.pipeline.to_gold`` — the single Gold implementation
(``idp.transforms.features``, REQ-ORC-05) shared with ``idp gold``, the Kubernetes
Job and Airflow. Requires the ``idp`` package in the Spark runtime (the idp image
bundles it). Lake location, lookback windows and Spark master/memory come from
config/platform.yaml + IDP_* env, exactly as for ``idp gold``.

  spark-submit pipelines/spark/gold_job.py [--config path/to/platform.yaml]
"""
from __future__ import annotations

import argparse
import json

from idp.common.config import load_config
from idp.pipeline import to_gold


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold stage (Silver -> Gold features)")
    parser.add_argument("--config", default=None, help="Path to platform.yaml")
    args = parser.parse_args()
    print(json.dumps(to_gold(load_config(args.config)), indent=2))


if __name__ == "__main__":
    main()
