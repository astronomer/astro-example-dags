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
    'email': ['john.hiett@broadcom.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=32),
}
with DAG(
    'expenses',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='30 12 * * 1-5',
    start_date=days_ago(1),
    catchup=False,
    tags=['orders'],
) as dag:
    t1 = BashOperator(
        task_id='extract_report',
        depends_on_past=False,
        bash_command='MIN=90 ; MAX=94 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
        sla=timedelta(seconds=95),
    )

    t2 = BashOperator(
        task_id='gather_user_data',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=200 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
        sla=timedelta(seconds=150),
    )

    t3 = BashOperator(
        task_id='gather_expense_data',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=240 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
        sla=timedelta(seconds=180),
    )

    t4 = BashOperator(
        task_id='gather_authorization_data',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=359 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t5 = BashOperator(
        task_id='verify_authorization',
        depends_on_past=False,
        bash_command='MIN=38 ; MAX=68 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t6 = BashOperator(
        task_id='submit_for_payment',
        depends_on_past=False,
        bash_command='MIN=80 ; MAX=150 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t1 >> [t2, t3, t4]
    [t2, t3, t4] >> t5
    t5 >> t6
