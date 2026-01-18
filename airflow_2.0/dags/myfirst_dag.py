from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id="MyFirstDAG",
    start_date=datetime(2026, 1, 18),
    schedule_interval="*/2 * * * *",  # every 2 minutes
)


def print_context(**kwargs):
    print(kwargs)
    print("Job completed")


copy_file = BashOperator(
    dag=dag,
    task_id="copy_file",
    bash_command="echo copying file from one location to another location",
)

task2 = PythonOperator(task_id="task2", python_callable=print_context, dag=dag)

copy_file >> task2
# copy_file.set_downstream(task2)
