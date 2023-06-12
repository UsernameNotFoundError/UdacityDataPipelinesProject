from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# Own Operators
import sys, os
sys.path.append(os.getcwd()+"/plugins/operators")
from StageToRedshiftOperator import StageToRedshiftOperator

def etl_start():
    print("Starting execution!")


default_args={
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "ETL",
    default_args=default_args,
    schedule_interval="@hourly",
)

begin_task = StageToRedshiftOperator(
            task_id="Begin_execution",
            dag=dag,
)

stage_events_task = DummyOperator(
            task_id="Stage_events",
            dag=dag,
)

stage_songs_task = DummyOperator(
            task_id="Stage_songs",
            dag=dag,
)

begin_task >> stage_events_task
begin_task.set_downstream(stage_songs_task)

load_songplays_fact_table_task = DummyOperator(
            task_id="Load_songplays_fact_table",
            dag=dag,
)

load_songplays_fact_table_task << stage_events_task
load_songplays_fact_table_task.set_upstream(stage_songs_task)