import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

# from plugins.utils.prefix_columns import prefix_columns
from plugins.utils.get_recursive_sql_file_lists import get_recursive_sql_file_lists

from plugins.operators.run_checksum_sql import RunChecksumSQLPostgresOperator

exported_schemas_path = "../include/exportedSchemas/"
exported_schemas_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), exported_schemas_path)

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
    "05_create_reports_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

# dag.user_defined_filters = {"prefix_columns": prefix_columns}

wait_for_functions = ExternalTaskSensor(
    task_id="wait_for_functions_to_complete",
    external_dag_id="04_create_functions_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

reports = "./sql/reports"
reports_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), reports)

reports_sql_files = get_recursive_sql_file_lists(reports_abspath, subdir="reports")

last_report_task = wait_for_functions
for group_index, group_list in enumerate(reports_sql_files, start=1):
    report_task = DummyOperator(task_id=f"reports_{group_index}", dag=dag)
    report_task_complete = DummyOperator(task_id=f"reports_{group_index}_complete", dag=dag)
    last_report_task >> report_task

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
            sql_type="report",
            json_schema_file_dir=exported_schemas_abspath,
            add_table_columns_to_context=["dim__time"],
            dag=dag,
        )
        # Add the current task to the array
        tasks_in_current_group.append(task)
    report_task >> tasks_in_current_group >> report_task_complete
    last_report_task = report_task_complete
