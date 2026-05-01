"""
fetch_market_data.py
--------------------
Main ingestion DAG. Runs every hour on weekdays.
Task flow:
  check_market_open → [ingest_AAPL, ingest_MSFT, ...] → trigger_dbt
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from sensors import MarketOpenSensor

load_dotenv(os.path.expanduser("~/github_repo/Stock-Market-Intelligence-Pipeline/.env"))

WATCHLIST = os.getenv("WATCHLIST", "AAPL,MSFT").split(",")

default_args = {
    "owner": "pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure": False,  # set True and configure SMTP if you want email alerts
}

with DAG(
    dag_id="fetch_market_data",
    default_args=default_args,
    description="Ingest 15-min OHLCV bars for all watchlist tickers",
    schedule_interval="*/60 * * * 1-5",  # every 60 min, Mon–Fri
    start_date=datetime(2026, 1, 1),
    catchup=False,          # IMPORTANT: don't backfill missed runs automatically
    max_active_runs=1,      # prevent overlapping runs
    tags=["ingestion", "market-data"],
) as dag:

    # Task 1: Check the market is open before doing anything
    check_market = MarketOpenSensor(
        task_id="check_market_open",
        poke_interval=900,   # re-check every 900 seconds
        timeout=3600,        # give up after 60 minutes (one full schedule interval)
        soft_fail=True,     # mark as SKIPPED on timeout
        mode="reschedule",  # releases the worker slot while waiting (efficient)
    )

    # Task 2: One ingest task per ticker (run in parallel)
    def make_ingest_task(symbol: str):
        import sys
        sys.path.insert(0, os.path.expanduser("~/github_repo/Stock-Market-Intelligence-Pipeline"))
        from ingestion.fetch_ohlcv import ingest_ticker

        def _run(**context):
            result = ingest_ticker(symbol)
            # Push result to XCom so it appears in Airflow UI logs
            context["ti"].xcom_push(key="result", value=result)

        return PythonOperator(
            task_id=f"ingest_{symbol}",
            python_callable=_run,
        )

    ingest_tasks = [make_ingest_task(sym) for sym in WATCHLIST]

    # Task 3: Trigger the dbt DAG once all ingest tasks complete
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_models",
        trigger_dag_id="run_dbt_models",
        wait_for_completion=False,  # don't block — let dbt run independently
    )

    # Wire the dependencies
    check_market >> ingest_tasks >> trigger_dbt
