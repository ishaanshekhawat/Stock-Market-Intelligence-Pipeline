from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="monthly_maintenance",
    schedule_interval="0 5 1 * *",  # 5am on the 1st of every month
    start_date=days_ago(1),
    catchup=False,
    tags=["maintenance"],
) as dag:

    create_partition = PostgresOperator(
        task_id="create_next_partition",
        postgres_conn_id="market_db",
        sql="""
            DO $$
            DECLARE
                next_month DATE := DATE_TRUNC('month', NOW() + INTERVAL '1 month')::DATE;
                month_end  DATE := (next_month + INTERVAL '1 month')::DATE;
                tbl_name   TEXT := 'fact_prices_' || TO_CHAR(next_month, 'YYYY_MM');
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = tbl_name) THEN
                    EXECUTE FORMAT(
                        'CREATE TABLE %I PARTITION OF fact_prices FOR VALUES FROM (%L) TO (%L)',
                        tbl_name, next_month, month_end
                    );
                END IF;
            END $$;
        """,
    )
