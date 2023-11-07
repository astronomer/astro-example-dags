from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello():
    print("Hello, world!")

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
}

dag = DAG(
    dag_id="hello_world",
    default_args=default_args,
    description="A simple DAG that prints 'hello world'",
    schedule_interval="@daily",
)

print_hello_task = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello,
    dag=dag,
)

print_hello_task
