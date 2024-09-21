import os

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator

from plugins.utils.is_latest_active_dagrun import is_latest_dagrun
from plugins.utils.get_recursive_sql_file_lists import get_recursive_sql_file_lists
from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

from plugins.operators.run_checksum_sql import RunChecksumSQLPostgresOperator


def run_dynamic_sql_task(
    dag,
    wait_for_task,
    sql_type,
    add_table_columns_to_context=[],
    check_entity_pattern=True,
):
    sql_dir = f"../../dags/sql/{sql_type}"
    sql_dir_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), sql_dir)

    exported_schemas_path = "../../include/exportedSchemas/"
    exported_schemas_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), exported_schemas_path)

    is_latest_dagrun_task = ShortCircuitOperator(
        task_id="skip_check",
        python_callable=is_latest_dagrun,
        depends_on_past=False,
        dag=dag,
    )

    sql_files = get_recursive_sql_file_lists(
        sql_dir_abspath,
        subdir=sql_type,
        add_table_columns_to_context=add_table_columns_to_context,
        check_entity_pattern=check_entity_pattern,
    )
    wait_for_task >> is_latest_dagrun_task
    last_task = is_latest_dagrun_task

    for group_index, group_list in enumerate(sql_files, start=1):
        task = DummyOperator(task_id=f"{sql_type}_{group_index}", dag=dag)
        task_complete = DummyOperator(task_id=f"{sql_type}_{group_index}_complete", dag=dag)
        last_task >> task

        # Initialize an array to hold all tasks in the current group
        tasks_in_current_group = []

        for config in group_list:
            id = config["filename"]
            run_task = RunChecksumSQLPostgresOperator(
                task_id=id,
                postgres_conn_id="postgres_datalake_conn_id",
                schema="public",
                filename=config["filename"],
                checksum=config["checksum"],
                sql=config["sql"],
                sql_type=sql_type,
                json_schema_file_dir=exported_schemas_abspath,
                add_table_columns_to_context=config["add_table_columns_to_context"],
                on_failure_callback=[send_harper_failure_notification()],
                dag=dag,
            )
            # Add the current task to the array
            tasks_in_current_group.append(run_task)
        task >> tasks_in_current_group >> task_complete
        last_task = task_complete
