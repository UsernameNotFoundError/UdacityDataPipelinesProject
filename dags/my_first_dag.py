# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html
from datetime import datetime, timedelta

# The DAG object is needed to instantiate a DAG
from airflow import DAG

# Operators
# An operator defines a unit of work for Airflow to complete
# All operators inherit from the BaseOperator
from airflow.operators.bash import BashOperator

with DAG(
    "my_first_dag", #  The dag_id, which serves as a unique identifier for your DAG
    # These args will get passed on to each operator
    # They can be overrided on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # Tasks determine how to execute your operator’s work within the context of a DAG
    t1 = BashOperator(
    task_id="print_date",
    bash_command="date",
    )
    
    t2 = BashOperator(
    task_id="sleep",
    depends_on_past=False,
    bash_command="sleep 5",
    retries=3,
    )
    
    t1 >> t2