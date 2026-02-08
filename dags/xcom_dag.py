from airflow.sdk.bases import xcom
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

# Confusing path -> We had below earlier. But it was creating circular import issues.
# So moved to a new file. Still have it for backward compatibility. We will remove it in future.
# from airflow.models.taskinstance import TaskInstance
# from airflow.sdk.execution_time.xcom import XCom

import requests
import json
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.timezone import utc

dag = DAG(
    dag_id="xcom_dag",
    start_date=datetime(2026, 1, 1),
    schedule=CronDataIntervalTimetable(
        cron="*/2 * * * *",  # every 2 minutes
        timezone=utc,
    ),
)


def product_page_callable(**context):
    ti = context["ti"]
    ti.xcom_push(key="output_path", value="/airflow/output_files/folder1/test.csv")
    print(
        "Pushed XCom with key 'output_path' and value '/airflow/output_files/folder1/test.csv'"
    )


product_page = PythonOperator(
    dag=dag,
    task_id="product_page",
    python_callable=product_page_callable,
)


def read_raw_callable(**context):
    ti = context["ti"]
    output_path = ti.xcom_pull(key="output_path", task_ids="product_page")
    print(f"Pulled XCom with key 'output_path' : {output_path}")


read_raw = PythonOperator(
    dag=dag,
    task_id="read_raw",
    python_callable=read_raw_callable,
)

product_page >> read_raw
