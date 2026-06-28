"""Airflow DAG for the IDP batch pipeline (plan Q5: Airflow orchestration).

Schedules the daily Bronze -> Silver -> Features -> ML -> Gold run. In a real
deployment each task would submit a Spark-on-K8s job; here the tasks call the
``idp`` package directly so the DAG is runnable in a lightweight setup too.

Deploy by mounting this file into the Airflow ``dags/`` folder.
"""
from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # allows importing/linting without Airflow installed
    DAG = None
    PythonOperator = None


def _generate(**_):
    from idp.cli import _cmd_generate
    import argparse
    _cmd_generate(argparse.Namespace(config=None))


def _ingest_curate_features(**_):
    # In production these are separate Spark jobs; combined here for clarity.
    from idp.common.config import load_config
    from idp.pipeline import run
    run(load_config(), persist=True)


def _refresh_models(**_):
    # Models are retrained inside run(); a real DAG would split train/score and
    # promote via MLflow. Kept as a marker task.
    return "models refreshed in pipeline run"


default_args = {
    "owner": "idp",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if DAG is not None:
    with DAG(
        dag_id="idp_batch_pipeline",
        description="Daily telecom lakehouse batch: ingest->curate->features->ML->gold",
        schedule="0 2 * * *",          # daily at 02:00
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["idp", "lakehouse", "ml"],
    ) as dag:
        generate = PythonOperator(task_id="generate_synthetic", python_callable=_generate)
        pipeline = PythonOperator(
            task_id="ingest_curate_features_score", python_callable=_ingest_curate_features
        )
        models = PythonOperator(task_id="refresh_models", python_callable=_refresh_models)

        generate >> pipeline >> models
