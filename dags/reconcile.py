import datetime as dt
from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=32),
}
with DAG(
    'reconcile',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='20 9-17 * * 1-5',
    start_date=days_ago(1),
    catchup=False,
    tags=['orders'],
) as dag:
    t1 = BashOperator(
        task_id='pre_processing',
        depends_on_past=False,
        bash_command='MIN=90 ; MAX=94 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
        sla=timedelta(seconds=95),
    )

    t2 = BashOperator(
        task_id='gather_data_1',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=200 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
        sla=timedelta(seconds=150),
    )

    t3 = BashOperator(
        task_id='gather_data_2',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=240 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
        sla=timedelta(seconds=180),
    )

    t4 = BashOperator(
        task_id='deduplicate',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=359 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t5 = BashOperator(
        task_id='verify',
        depends_on_past=False,
        bash_command='MIN=38 ; MAX=168 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t6 = BashOperator(
        task_id='post_processing',
        depends_on_past=False,
        bash_command='MIN=80 ; MAX=150 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t1 >> [t2, t3]
    [t2, t3] >> t4
    t4 >> t5
    t5 >> t6
