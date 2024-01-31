from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.bash_operator import BashOperator  # Correct import

default_args = {
    'owner': 'airflow'
}

dag = DAG(
    dag_id='s3',  # Changed dag_id to use underscores instead of spaces
    description='our first "hello world" DAG!',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once'  # Changed schedule_interval to a valid value
)

task = BashOperator(
    task_id='hello_world_task',  # Changed task_id to use underscores instead of spaces
    bash_command='echo hello world!',
    dag=dag
)

task  # This line is not needed

