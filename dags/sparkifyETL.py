from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

dag = DAG(
    "ETL",
    description="A simple DAG",
    start_date=datetime(2023, 5, 1),
    schedule_interval="@daily",
)

begin_task = PythonOperator(
            task_id="Begin_execution",
            python_callable=hello_world,
            dag=dag,
)

stage_events_task = PythonOperator(
            task_id="Stage_events",
            python_callable=hello_world,
            dag=dag,
)

stage_songs_task = PythonOperator(
            task_id="Stage_songs",
            python_callable=hello_world,
            dag=dag,
)

begin_task >> stage_events_task
begin_task.set_downstream(stage_songs_task)

load_songplays_fact_table_task = PythonOperator(
            task_id="Load_songplays_fact_table",
            python_callable=hello_world,
            dag=dag,
)

load_songplays_fact_table_task << stage_events_task
load_songplays_fact_table_task.set_upstream(stage_songs_task)