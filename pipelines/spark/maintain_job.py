"""Maintenance stage entrypoint for spark-submit / the Spark Operator (compaction, snapshot expiry, orphans).

Thin wrapper over ``idp maintain`` (``idp.cli``), so the SparkApplication runs the
same code as the CLI, the Kubernetes Job and Airflow (REQ-ORC-05). Lake location,
catalog and resources come from config/platform.yaml + IDP_* env.

  spark-submit pipelines/spark/maintain_job.py
"""
from __future__ import annotations

import sys

from idp.cli import main

if __name__ == "__main__":
    sys.exit(main(["maintain", *sys.argv[1:]]))
