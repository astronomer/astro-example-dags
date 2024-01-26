from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Using the 'with' statement for the DAG definition
with DAG(
    'dummy_dag_2',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    tags=['example'],
) as dag:

    task1 = DummyOperator(
        task_id='start',
    )

    task2 = DummyOperator(
        task_id='task2',
    )

    task3 = DummyOperator(
        task_id='task3',
    )

    task4 = DummyOperator(
        task_id='end',
    )

    task1 >> task2 >> task3 >> task4
