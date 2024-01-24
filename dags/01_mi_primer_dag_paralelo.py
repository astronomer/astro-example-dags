from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime , timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Datapath',
    'depends_on_past': False,
    'email_on_failure': 'moralesmeza28@gmail.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def start_process():
    print(" INICIO EL PROCESO!")

def load_master():
    print(" Load Master!")

def load_raw_2():
    print(" Hola Raw 2!")

def load_raw_1():
    print(" Hola Raw 1!")

with DAG(
    dag_id="mi_primer_dag_paralelo",
    schedule="20 04 * * *", 
    start_date=days_ago(2), 
    default_args=default_args,
    description='Prueba de Dag'
) as dag:
    step_start = PythonOperator(
        task_id='step_start',
        python_callable=start_process,
        dag=dag
    )
    step_load_raw_1 = PythonOperator(
        task_id='step_load_raw_1',
        python_callable=load_raw_1,
        dag=dag
    )
    step_load_raw_2 = PythonOperator(
        task_id='step_load_raw_2',
        python_callable=load_raw_2,
        dag=dag
    )
    step_master = PythonOperator(
        task_id='step_master',
        python_callable=load_master,
        dag=dag
    )
    step_start>>step_load_raw_1
    step_start>>step_load_raw_2
    step_load_raw_1>>step_master
    step_load_raw_2>>step_master