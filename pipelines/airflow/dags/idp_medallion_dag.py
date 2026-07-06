"""Airflow DAG orchestrating the medallion stages (production mode for Silver/Gold).

Each stage is an independent task that reads its input layer from S3 and writes
its output layer — the same separation of concerns as the on-demand CLI
(`idp silver`, `idp gold`). In a real deployment these tasks would submit Spark
jobs (SparkSubmitOperator) or pods (KubernetesPodOperator); here they call the
`idp` stage functions directly so the DAG is runnable in a lightweight setup too.

Schedule: Silver/Gold refresh hourly off the Bronze that the streaming consumer
lands continuously. Model retraining runs daily.

Deploy: mount this file into the Airflow `dags/` folder.
"""
from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # allow import/lint without Airflow installed
    DAG = None
    PythonOperator = None


def _silver(**_):
    from idp.common.config import load_config
    from idp.pipeline import to_silver
    to_silver(load_config(), persist=True)          # Bronze (S3) -> Silver (S3)


def _gold(**_):
    from idp.common.config import load_config
    from idp.pipeline import to_gold
    to_gold(load_config(), persist=True)            # Silver (S3) -> Gold (S3)


def _train(**_):
    from idp.common.config import load_config
    from idp.pipeline import train
    train(load_config(), persist=True)              # labelled data -> models


default_args = {"owner": "idp", "retries": 2, "retry_delay": timedelta(minutes=5)}

if DAG is not None:
    # Hourly Silver -> Gold refresh.
    with DAG(
        dag_id="idp_silver_gold",
        description="Medallion refresh: Bronze -> Silver -> Gold (reads/writes S3)",
        schedule="0 * * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["idp", "medallion"],
    ) as dag_refresh:
        silver = PythonOperator(task_id="silver", python_callable=_silver)
        gold = PythonOperator(task_id="gold", python_callable=_gold)
        silver >> gold

    # Daily model retraining (separate concern from the ELT).
    with DAG(
        dag_id="idp_train_models",
        description="Offline ML training on labelled data",
        schedule="0 2 * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["idp", "ml"],
    ) as dag_train:
        PythonOperator(task_id="train", python_callable=_train)
