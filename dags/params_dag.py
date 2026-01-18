from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


def print_params(**context):
    print(context["params"])


with DAG(
    dag_id="params_dag",
    start_date=datetime(2025, 12, 5),
    schedule=None,
    params={"env": "dev", "load_type": "full", "retry_count": 3},
) as dag:

    task = PythonOperator(task_id="print_params", python_callable=print_params)
