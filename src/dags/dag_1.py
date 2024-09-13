from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from datetime import datetime, timedelta

postgres_conn_id = "postgres_conn_id"

vertica_conn_id = "vertica_conn_id"

def load_data_to_staging(**kwargs):
    execution_date = kwargs["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema="db1")
    vertica_hook = VerticaHook(vertica_conn_id=vertica_conn_id)

    sql_transactions = f"""
        INSERT INTO STV2024050734__STAGING.transactions (
            operation_id, account_number_from, account_number_to, currency_code, country, status, 
            transaction_type, amount, transaction_dt, transaction_dt_year, transaction_dt_month, 
            transaction_dt_day, transaction_hash
        )
        SELECT * FROM public.transactions 
        WHERE transaction_dt >= '{date_str}';
    """

    sql_currencies = f"""
        INSERT INTO STV2024050734__STAGING.currencies (
            date_update, currency_code, currency_code_with, currency_with_div, 
            date_update_year, date_update_month, date_update_day, currency_hash
        )
        SELECT * FROM public.currencies 
        WHERE date_update >= '{date_str}';
    """

    vertica_hook.run(sql_transactions)
    vertica_hook.run(sql_currencies)
with DAG(
    dag_id="load_data_to_staging",
    start_date=datetime(2022, 10, 1),
    schedule_interval="@daily",
    catchup=True,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["vertica", "staging", "data_load"],
) as dag:

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_to_staging,
    )

    load_data
