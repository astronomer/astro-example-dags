from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.run_dynamic_sql_task import run_dynamic_sql_task

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    # "email": ["martin@harperconcierge.com"],
    # "email_on_failure": True,
    # "email_on_retry": False,
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
}


sql_type = "dimensions"
dag = DAG(
    f"02_create_{sql_type}_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

wait_for_task = ExternalTaskSensor(
    task_id="wait_for_migrations_to_import",
    external_dag_id="01_mongo_migrations_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

run_dynamic_sql_task(dag, wait_for_task, sql_type)
