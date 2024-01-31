from datetime import datetime , timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.bash import BashOperator

defalu_args={
  'owner'= 'Sachin Tungaria'
}
dag= DAG(
  dag_id='hello world',
  description='our first "hello world" DAG!',
  default_args=default_args,
  start_date=days_ago(1),
  schedule_interval=1
)
task= BashOpeartor(
  task_id='hello world task',
  bash_command='echo hello world!',
  dag=dag
)

task
