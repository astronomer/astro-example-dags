from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.operators.python import BranchPythonOperator 
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime , timedelta
from airflow.utils.dates import days_ago
from datetime import date 

default_args = {
    'owner': 'Datapath',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
    
def fun_load_raw():
    print(" Se cargo raw1!")

def fun_load_master_complete():
    print(" Se cargo load_master_complete!")

   
    
def fun_load_master_delta():
    print(" Se cargo Master Delta!")
    
def fun_branch():
    today = date.today()
    if today.weekday()==1 :
        return "load_master_complete"
    else :
        return "load_master_delta"
        
def fun_load_bi():
    print(" Hola Airflow!")
    

with DAG(
    dag_id="load_branch", 
    schedule="20 04 * * *", 
    start_date=days_ago(2), 
    default_args=default_args,
    description='Prueba de Dag'
) as dag:
    load_raw = PythonOperator(
        task_id='load_raw',
        python_callable=fun_load_raw,
        dag=dag
    )
    
    branch = BranchPythonOperator( 
    task_id='branch', 
    python_callable=fun_branch, 
    provide_context=True, 
    dag=dag
) 

    load_master_complete = PythonOperator(
        task_id='load_master_complete',
        python_callable=fun_load_master_complete,
        dag=dag
    )
    load_master_delta = PythonOperator(
        task_id='load_master_delta',
        python_callable=fun_load_master_delta,
        dag=dag
    )
    load_bi = PythonOperator(
        task_id='load_bi',
        python_callable=fun_load_bi,
        dag=dag
    )
    load_raw>>branch>>[load_master_complete,load_master_delta]>>load_bi