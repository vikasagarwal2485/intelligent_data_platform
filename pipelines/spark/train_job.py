"""Train stage entrypoint for spark-submit / the Spark Operator (Gold features + labels -> models).

Thin wrapper over ``idp train`` (``idp.cli``), so the SparkApplication runs the
same code as the CLI (REQ-ORC-05). The join and the stratified sample run on the
executors; the driver collects the bounded sample (REQ-ML-09), fits the models and
saves them through the model store (REQ-ML-04). Lake location, catalog, registry
and resources come from config/platform.yaml + IDP_* env.

  spark-submit pipelines/spark/train_job.py
"""
from __future__ import annotations

import sys

from idp.cli import main

if __name__ == "__main__":
    sys.exit(main(["train", *sys.argv[1:]]))
