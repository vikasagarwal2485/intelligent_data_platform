"""Silver stage entrypoint for spark-submit / the Spark Operator (Bronze -> Silver).

Thin wrapper over ``idp silver`` (``idp.cli``) -> ``idp.pipeline.to_silver``, the
single Silver implementation (``idp.transforms``, REQ-ORC-05) shared with the CLI,
the Kubernetes Job and Airflow. Arguments pass through, e.g. ``--full-refresh``.
Lake location, catalog and resources come from config/platform.yaml + IDP_* env.

  spark-submit pipelines/spark/silver_job.py [--full-refresh]
"""
from __future__ import annotations

import sys

from idp.cli import main

if __name__ == "__main__":
    sys.exit(main(["silver", *sys.argv[1:]]))
