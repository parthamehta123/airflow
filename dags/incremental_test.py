from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


def fetch_incremental_data(**context):
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    print(f"Fetching incremental data from {start} to {end}")


with DAG(
    dag_id="incremental_api_30min",
    start_date=datetime(2026, 1, 2, 11, 10),
    schedule="*/2 * * * *",  # every 2 minutes
    catchup=True,
):
    PythonOperator(
        task_id="fetch_api",
        python_callable=fetch_incremental_data,
    )
