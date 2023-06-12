from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

dag = DAG(
    "my_dag",
    description="A simple DAG",
    start_date=datetime(2023, 5, 1),
    schedule_interval="@daily",
)

task = PythonOperator(
    task_id="hello_task",
    python_callable=hello_world,
    dag=dag,
)
