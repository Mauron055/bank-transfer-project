from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from datetime import datetime, timedelta

vertica_conn_id = "vertica_conn_id"


def update_data_warehouse(**kwargs):
    execution_date = kwargs["execution_date"]
    yesterday = execution_date - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    hook = VerticaHook(vertica_conn_id=vertica_conn_id)

    sql_clean_transactions = f"""
        DELETE FROM STV2024050734__STAGING.transactions
        WHERE account_number_from < 0 OR account_number_to < 0;
    """

    sql_update_dwh = f"""
        INSERT INTO STV2024050734__DWH.global_metrics (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
        SELECT 
            CAST('{date_str}' AS DATE) AS date_update,
            currency_code,
            SUM(amount) AS amount_total,
            COUNT(*) AS cnt_transactions,
            AVG(amount) AS avg_transactions_per_account,
            COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
        FROM STV2024050734__STAGING.transactions
        WHERE transaction_dt >= '{date_str}'
        GROUP BY currency_code;
    """

    hook.run(sql_clean_transactions)
    hook.run(sql_update_dwh)


with DAG(
    dag_id="update_data_warehouse",
    start_date=datetime(2022, 10, 1),
    schedule_interval="@daily",
    catchup=True,
    default_args={"retries": 3},
    tags=["vertica", "dwh", "data_update"],
) as dag:

    update_dwh = PythonOperator(
        task_id="update_dwh",
        python_callable=update_data_warehouse,
    )

    update_dwh
