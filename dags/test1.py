from datetime import datetime
from airflow.sdk import dag, task
from airflow.sdk import get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum


@dag(
    start_date=datetime(2026, 1, 3),
    schedule=CronDataIntervalTimetable(
        "0 * * * *", timezone=pendulum.timezone("Asia/Kolkata")  # every hour
    ),
    # schedule="@daily",
    catchup=True,
    is_paused_upon_creation=False,
)
def print_execution_dates_v2():

    @task
    def print_actual_interval():
        context = get_current_context()

        start = context["data_interval_start"]
        end = context["data_interval_end"]

        dag_run = context["dag_run"]
        logical = dag_run.logical_date
        run_after = dag_run.run_after

        print(f"Period Start: {start}")
        print(f"Period End: {end}")
        print(f"Logical Date: {logical}")
        print(f"Run After: {run_after}")

    @task
    def debug_timetable():
        context = get_current_context()
        dag = context["dag"]
        print(type(dag.timetable))

    print_actual_interval() >> debug_timetable()


print_execution_dates_v2()
