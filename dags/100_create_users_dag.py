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
    "100_create_users_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

wait_for_reports = ExternalTaskSensor(
    task_id="wait_for_migrations_to_complete",
    external_dag_id="01_mongo_migrations_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

users = "./sql/users"
users_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), users)

users_sql_files = get_recursive_sql_file_lists(users_abspath, subdir="users")

last_user_task = wait_for_reports
for group_index, group_list in enumerate(users_sql_files, start=1):
    user_task = DummyOperator(task_id=f"users_{group_index}", dag=dag)
    user_task_complete = DummyOperator(task_id=f"users_{group_index}_complete", dag=dag)
    last_user_task >> user_task

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
            sql_type="user",
            dag=dag,
        )
        # Add the current task to the array
        tasks_in_current_group.append(task)
    user_task >> tasks_in_current_group >> user_task_complete
    last_user_task = user_task_complete
