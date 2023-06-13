from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
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
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    "ETL",
    default_args=default_args,
    schedule_interval="@hourly",
)

begin_task = DummyOperator(
            task_id="Begin_execution",
            dag=dag,
)

# Change Bucket and Key if necessary
stage_events_task = StageToRedshiftOperator(
            task_id="Stage_events",
            redshift_conn_id = "redshift",
            aws_credentials_id = "aws_credentials",
            s3_bucket_name = "airflow-bucket-xx",
            s3_key = "data-pipelines/project/log-data/",
            dag=dag,
)

# Change Bucket and Key if necessary
stage_songs_task = StageToRedshiftOperator(
            task_id="Stage_songs",
            redshift_conn_id = "redshift",
            aws_credentials_id = "aws_credentials",
            s3_bucket_name = "airflow-bucket-xx",
            s3_key = "data-pipelines/project/song-data/",
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

load_song_task = DummyOperator(
            task_id="Load_song_dim_table",
            dag=dag,
)

load_user_task = DummyOperator(
            task_id="Load_user_dim_table",
            dag=dag,
)

load_artist_task = DummyOperator(
            task_id="Load_artist_dim_table",
            dag=dag,
)

load_time_task = DummyOperator(
            task_id="Load_time_dim_table",
            dag=dag,
)

load_songplays_fact_table_task.set_downstream(load_song_task)
load_songplays_fact_table_task.set_downstream(load_user_task)
load_songplays_fact_table_task.set_downstream(load_artist_task)
load_songplays_fact_table_task.set_downstream(load_time_task)

quality_check_task = DummyOperator(
            task_id="Run_data_quality_checks",
            dag=dag,
)

quality_check_task.set_upstream(load_song_task)
quality_check_task.set_upstream(load_user_task)
quality_check_task.set_upstream(load_artist_task)
quality_check_task.set_upstream(load_time_task)

end_task = StageToRedshiftOperator(
            task_id="End_execution",
            dag=dag,
)

quality_check_task >> end_task