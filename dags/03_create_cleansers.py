import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.get_recursive_sql_file_lists import get_recursive_sql_file_lists

from plugins.operators.run_checksum_sql import RunChecksumSQLPostgresOperator

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


dag = DAG(
    "03_create_cleansers_dag",
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

cleansers = "./sql/cleansers"
cleansers_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), cleansers)

exported_schemas_path = "../include/exportedSchemas/"
exported_schemas_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), exported_schemas_path)

sql_files = get_recursive_sql_file_lists(cleansers_abspath, subdir="cleansers")

wait_for_financials >> wait_for_dimensions

last_index_task = wait_for_dimensions
for group_index, group_list in enumerate(sql_files, start=1):
    index_task = DummyOperator(task_id=f"cleansers_{group_index}", dag=dag)
    index_task_complete = DummyOperator(task_id=f"cleansers_{group_index}_complete", dag=dag)
    last_index_task >> index_task

    # Initialize an array to hold all tasks in the current group
    tasks_in_current_group = []

    for config in group_list:
        id = config["filename"]
        task = RunChecksumSQLPostgresOperator(
            task_id=id,
            postgres_conn_id="postgres_datalake_conn_id",
            schema="public",
            filename=config["filename"],
            checksum=config["checksum"],
            sql=config["sql"],
            sql_type="cleanser",
            json_schema_file_dir=exported_schemas_abspath,
            add_table_columns_to_context=["dim__time"],
            dag=dag,
        )
        # Add the current task to the array
        tasks_in_current_group.append(task)
    index_task >> tasks_in_current_group >> index_task_complete
    last_index_task = index_task_complete
