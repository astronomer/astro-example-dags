from airflow.decorators import dag, task
from datetime import datetime
from airflow.models.baseoperator import chain

@dag('my_task_api_dag', 
        start_date=datetime(2023,1,1), 
        description='My First Task API DAG',
        tags=['training', 'onboarding'],
        schedule='@daily', 
        catchup=False)

def my_task_api_dag():
    
    @task
    def print_a():
        print('hi from task a')

    @task
    def print_b():
        print('hi from task b')

    #print_a() >> print_b()
    chain(print_a(), print_b())
 
my_task_api_dag()