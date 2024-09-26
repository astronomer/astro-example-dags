"""
this is for testing
"""

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

#Define the basic parameters of the DAG, like schedule and start_date
default_args = {
    "owner": "Astro",
    "start_date": days_ago(1),
    "retries": 3,
}

def start():
    print("I am in start")
    
def end():
    print("I am in end")

with DAG(
    dag_id="my_first_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["myfirstdag"],
) as dag:
    start_task = PythonOperator(
        task_id='start',
        python_callable=start,
    )

    end_task = PythonOperator(
        task_id='end',
        python_callable=end,
    )

start_task >> end_task
