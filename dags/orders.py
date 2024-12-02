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
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'orders',
    default_args=default_args,
    description='A simple example DAG',
#   schedule_interval=timedelta(minutes=120),
    schedule_interval='15 8-22 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['orders'],
) as dag:
    t1 = BashOperator(
        task_id='orders_t1',
        depends_on_past=False,
        bash_command='MIN=90 ; MAX=94 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
        sla=timedelta(seconds=95),
    )

    t2 = BashOperator(
        task_id='orders_t2',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=200 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
        sla=timedelta(seconds=30),
    )

    t3 = BashOperator(
        task_id='orders_t3',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=240 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t4 = BashOperator(
        task_id='orders_t4',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=359 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t5 = BashOperator(
        task_id='orders_t5',
        depends_on_past=False,
        bash_command='MIN=38 ; MAX=168 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t6 = BashOperator(
        task_id='orders_t6',
        depends_on_past=False,
        bash_command='MIN=80 ; MAX=150 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t7 = BashOperator(
        task_id='orders_t7',
        depends_on_past=False,
        bash_command='MIN=90 ; MAX=160 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t8 = BashOperator(
        task_id='orders_t8',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=170 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t9 = BashOperator(
        task_id='orders_t9',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=76 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t10 = BashOperator(
        task_id='orders_t10',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=145 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t11 = BashOperator(
        task_id='orders_t11',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=120 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t12 = BashOperator(
        task_id='orders_t12',
        depends_on_past=False,
        bash_command='MIN=230 ; MAX=266 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t13 = BashOperator(
        task_id='orders_t13',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=120 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t14 = BashOperator(
        task_id='orders_t14',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=180 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t15 = BashOperator(
        task_id='orders_t15',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=240 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t16 = BashOperator(
        task_id='orders_t16',
        depends_on_past=False,
        bash_command='MIN=66 ; MAX=86 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t17 = BashOperator(
        task_id='orders_t17',
        depends_on_past=False,
        bash_command='MIN=130 ; MAX=240 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t18 = BashOperator(
        task_id='orders_t18',
        depends_on_past=False,
        bash_command='MIN=130 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t19 = BashOperator(
        task_id='orders_t19',
        depends_on_past=False,
        bash_command='MIN=70 ; MAX=90 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t20 = BashOperator(
        task_id='orders_t20',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=180 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t21 = BashOperator(
        task_id='orders_t21',
        depends_on_past=False,
        bash_command='MIN=130 ; MAX=240 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t22 = BashOperator(
        task_id='orders_t22',
        depends_on_past=False,
        bash_command='MIN=130 ; MAX=300 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t23 = BashOperator(
        task_id='orders_t23',
        depends_on_past=False,
        bash_command='MIN=70 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t24 = BashOperator(
        task_id='orders_t24',
        depends_on_past=False,
        bash_command='MIN=130 ; MAX=160 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t1 >> [t2, t3, t4]
    t2 >> t5
    t3 >> t6
    t4 >> t7
    t5 >> t8
    t6 >> t9
    t7 >> t10
    t8 >> t11
    t9 >> [t11, t12]
    t10 >> t12
    t11 >> t13
    t12 >> t14
    t13 >> [t15, t16]
    t14 >> [t16, t17]
    t15 >> t18
    t16 >> t19
    t17 >> t20
    [t18, t19, t20] >> t21
    t21 >> [t22, t23]
    [t22, t23] >> t24    
