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
    'hiport',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(minutes=120),
    start_date=days_ago(1),
    catchup=False,
    tags=['hiport'],
) as dag:
    t1 = BashOperator(
        task_id='hiport_t1',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=90 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t2 = BashOperator(
        task_id='hiport_t2',
        depends_on_past=False,
        bash_command='MIN=20 ; MAX=49 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t3 = BashOperator(
        task_id='hiport_t3',
        depends_on_past=False,
        bash_command='MIN=20 ; MAX=40 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t4 = BashOperator(
        task_id='hiport_t4',
        depends_on_past=False,
        bash_command='MIN=20 ; MAX=59 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t5 = BashOperator(
        task_id='hiport_t5',
        depends_on_past=False,
        bash_command='MIN=38 ; MAX=68 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t6 = BashOperator(
        task_id='hiport_t6',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=50 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t7 = BashOperator(
        task_id='hiport_t7',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=60 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t8 = BashOperator(
        task_id='hiport_t8',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=70 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t9 = BashOperator(
        task_id='hiport_t9',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=76 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t10 = BashOperator(
        task_id='hiport_t10',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=45 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t11 = BashOperator(
        task_id='hiport_t11',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=50 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t12 = BashOperator(
        task_id='hiport_t12',
        depends_on_past=False,
        bash_command='MIN=230 ; MAX=266 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t13 = BashOperator(
        task_id='hiport_t13',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=99 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t14 = BashOperator(
        task_id='hiport_t14',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=99 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t15 = BashOperator(
        task_id='hiport_t15',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=99 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t16 = BashOperator(
        task_id='hiport_t16',
        depends_on_past=False,
        bash_command='MIN=66 ; MAX=86 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t17 = BashOperator(
        task_id='hiport_t17',
        depends_on_past=False,
        bash_command='MIN=130 ; MAX=240 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t18 = BashOperator(
        task_id='hiport_t18',
        depends_on_past=False,
        bash_command='MIN=130 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t19 = BashOperator(
        task_id='hiport_t19',
        depends_on_past=False,
        bash_command='MIN=70 ; MAX=90 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t20 = BashOperator(
        task_id='hiport_t20',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t21 = BashOperator(
        task_id='hiport_t21',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t22 = BashOperator(
        task_id='hiport_t22',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t23 = BashOperator(
        task_id='hiport_t23',
        depends_on_past=False,
        bash_command='MIN=70 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t24 = BashOperator(
        task_id='hiport_t24',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=60 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t25 = BashOperator(
        task_id='hiport_t25',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=60 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t26 = BashOperator(
        task_id='hiport_t26',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=60 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t27 = BashOperator(
        task_id='hiport_t27',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=80 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t28 = BashOperator(
        task_id='hiport_t28',
        depends_on_past=False,
        bash_command='MIN=45 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t29 = BashOperator(
        task_id='hiport_t29',
        depends_on_past=False,
        bash_command='MIN=45 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t30 = BashOperator(
        task_id='hiport_t30',
        depends_on_past=False,
        bash_command='MIN=45 ; MAX=140 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t31 = BashOperator(
        task_id='hiport_t31',
        depends_on_past=False,
        bash_command='MIN=130 ; MAX=165 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t32 = BashOperator(
        task_id='hiport_t32',
        depends_on_past=False,
        bash_command='MIN=180 ; MAX=240 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t33 = BashOperator(
        task_id='hiport_t33',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=70 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t34 = BashOperator(
        task_id='hiport_t34',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=70 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t35 = BashOperator(
        task_id='hiport_t35',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=70 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t36 = BashOperator(
        task_id='hiport_t36',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=70 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t37 = BashOperator(
        task_id='hiport_t37',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=70 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t38 = BashOperator(
        task_id='hiport_t38',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=180 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t39 = BashOperator(
        task_id='hiport_t39',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=180 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t40 = BashOperator(
        task_id='hiport_t40',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=180 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t41 = BashOperator(
        task_id='hiport_t41',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=55 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t42 = BashOperator(
        task_id='hiport_t42',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=80 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t43 = BashOperator(
        task_id='hiport_t43',
        depends_on_past=False,
        bash_command='MIN=38 ; MAX=58 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t44 = BashOperator(
        task_id='hiport_t44',
        depends_on_past=False,
        bash_command='MIN=25 ; MAX=45 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t45 = BashOperator(
        task_id='hiport_t45',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=60 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t46 = BashOperator(
        task_id='hiport_t46',
        depends_on_past=False,
        bash_command='MIN=30 ; MAX=60 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t47 = BashOperator(
        task_id='hiport_t47',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=100 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t48 = BashOperator(
        task_id='hiport_t48',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=100 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t49 = BashOperator(
        task_id='hiport_t49',
        depends_on_past=False,
        bash_command='MIN=120 ; MAX=180 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t50 = BashOperator(
        task_id='hiport_t50',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=95 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t51 = BashOperator(
        task_id='hiport_t51',
        depends_on_past=False,
        bash_command='MIN=50 ; MAX=60 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t52 = BashOperator(
        task_id='hiport_t52',
        depends_on_past=False,
        bash_command='MIN=40 ; MAX=70 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t53 = BashOperator(
        task_id='hiport_t53',
        depends_on_past=False,
        bash_command='MIN=40 ; MAX=60 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t54 = BashOperator(
        task_id='hiport_t54',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=80 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t55 = BashOperator(
        task_id='hiport_t55',
        depends_on_past=False,
        bash_command='MIN=60 ; MAX=90 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t56 = BashOperator(
        task_id='hiport_t56',
        depends_on_past=False,
        bash_command='MIN=90 ; MAX=120 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )

    t57 = BashOperator(
        task_id='hiport_t57',
        depends_on_past=False,
        bash_command='MIN=70 ; MAX=90 ; date "+%d/%m/%y %H:%M:%S" ; sleep $((MIN+RANDOM % (MAX-MIN))) ; date "+%d/%m/%y %H:%M:%S"',
        retries=3,
    )
 
#    t1 >> [t2, t3] >> t4 >> t5 >> [t6, t7, t8] >> t9 >> [t10, t11] >> t12 >> [t13, t14, t15] >> t16 >> t17 >> t18 >> t19 >> [t20, t21, t22, t23] >> [t24, t25, t26] >> t27 >> [t28, t29, t30] >> t31 >> t32 >> [t33, t34, t35, t36, t37] >> [t38, t39, t40] >> t41 >> t42 >> t43 >> t44 >> [t45, t46] >> [t47, t48] >> t49 >> t50 >> [t51, t52, t53] >> [t54, t55] >> t56 >> t57
   
    t1 >> [t2, t3]
    [t2, t3] >> t4
    t4 >> t5
    t5 >> [t6, t7, t8]
    [t6, t7, t8] >> t9
    t9 >> [t10, t11]
    [t10, t11] >> t12
    t12 >> [t13, t14, t15]
    [t13, t14, t15] >> t16
    t16 >> t17
    t17 >> t18
    t18 >> t19
    t19 >> [t20, t21, t22, t23]
    t20 >> [t24, t25, t26]
    t21 >> [t24, t25, t26]
    t22 >> [t24, t25, t26]
    t23 >> [t24, t25, t26]
    [t24, t25, t26] >> t27
    t27 >> [t28, t29, t30]
    [t28, t29, t30] >> t31
    t31 >> t32
    t32 >> [t33, t34, t35, t36, t37]
    t33 >> [t38, t39, t40]
    t34 >> [t38, t39, t40]
    t35 >> [t38, t39, t40]
    t36 >> [t38, t39, t40]
    t37 >> [t38, t39, t40]
    [t38, t39, t40] >> t41
    t41 >> t42
    t42 >> t43
    t43 >> t44
    t44 >> [t45, t46]
    t45 >> [t47, t48]
    t46 >> [t47, t48]
    [t47, t48] >> t49
    t49 >> t50
    t50 >> [t51, t52, t53]
    t51 >> [t54, t55]
    t52 >> [t54, t55]
    t53 >> [t54, t55]
    [t54, t55] >> t56
    t56 >> t57
