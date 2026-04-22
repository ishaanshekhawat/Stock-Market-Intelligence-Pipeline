"""
run_dbt_models.py
-----------------
Runs dbt models in the correct sequence after each ingestion cycle.
Uses BashOperator to shell out to dbt CLI.
"""

import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DBT_DIR    = "/home/ubuntu/github_repo/Stock-Market-Intelligence-Pipeline/dbt_project"
VENV_DBT   = "/home/ubuntu/github_repo/Stock-Market-Intelligence-Pipeline/venv/bin/dbt"

default_args = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="run_dbt_models",
    default_args=default_args,
    description="Run dbt transformations after each ingestion cycle",
    schedule_interval=None,  # only triggered by fetch_market_data, not on schedule
    start_date=days_ago(1),
    catchup=False,
    tags=["transformation", "dbt"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {VENV_DBT} run --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {VENV_DBT} test --profiles-dir {DBT_DIR}",
    )

    dbt_run >> dbt_test
