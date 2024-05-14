from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.run_dynamic_sql_task import run_dynamic_sql_task

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
}


sql_type = "cleansers"

dag = DAG(
    f"03_create_{sql_type}_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

wait_for_financials = ExternalTaskSensor(
    task_id="wait_for_finance_to_complete",
    external_dag_id="02_import_financial_transactions_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

wait_for_dimensions = ExternalTaskSensor(
    task_id="wait_for_dimensions_to_complete",
    external_dag_id="02_create_dimensions_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

wait_for_financials >> wait_for_dimensions

run_dynamic_sql_task(
    dag,
    wait_for_task=wait_for_dimensions,
    sql_type=sql_type,
    add_table_columns_to_context=["dim__time"],
)
