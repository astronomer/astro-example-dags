from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.send_harper_slack_notification import (
    send_harper_failure_notification,
    send_harper_success_notification,
)

try:
    device_name = Variable.get("DEVICE_NAME")
except KeyError:
    device_name = "Production"  # Default value


sql_type = "reports"
if device_name != "Production":
    external_dag_id = f"55_create_{sql_type}_dag"
else:
    external_dag_id = "60_create_google_sheets_dag"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
    "on_failure_callback": [send_harper_failure_notification()],
}

sql_type = "reports"

dag = DAG(
    "daily_dagrun_successful",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
    on_success_callback=[send_harper_success_notification()],
)

# dag.user_defined_filters = {"prefix_columns": prefix_columns}

wait_for_task = ExternalTaskSensor(
    task_id="wait_for_all_dagruns_to_complete",
    external_dag_id=external_dag_id,  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

dags_completed = DummyOperator(task_id="all_dagruns_completed", dag=dag)

wait_for_task >> dags_completed
