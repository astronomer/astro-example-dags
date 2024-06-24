from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.utils.dates import days_ago
from time import time_ns
from datetime import datetime


def start_process():
    print(" INICIO EL PROCESO!")

def load():
    print(" Hola Airflow!")

with DAG(
    dag_id="mi_primer_dag", schedule="@once",
    start_date=days_ago(1), 
    description='Prueba de Dag'
) as dag:
    step_start = PythonOperator(
        task_id='step_start',
        python_callable=start_process,
        dag=dag
    )
    step_load = PythonOperator(
        task_id='step_load',
        python_callable=load,
        dag=dag
    )
    step_start>>step_load