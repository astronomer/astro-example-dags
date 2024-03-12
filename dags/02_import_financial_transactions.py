from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.operators.stripe_to_postgres_operator import StripeToPostgresOperator
from plugins.operators.zettle_to_postgres_operator import ZettleToPostgresOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
}


dag = DAG(
    "02_import_financial_transactions_dag",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

wait_for_migrations = ExternalTaskSensor(
    task_id="wait_for_migrations_to_complete",
    external_dag_id="01_mongo_migrations_dag",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)

stripe_task = StripeToPostgresOperator(
    task_id="import_stripe_transactions_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    stripe_conn_id="stripe_conn_id",
    destination_schema="transient_data",
    destination_table="stripe__charges",
    dag=dag,
)
zettle_task = ZettleToPostgresOperator(
    task_id="import_zettle_transactions_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    zettle_conn_id="zettle_conn_id",
    destination_schema="transient_data",
    destination_table="zettle__charges",
    dag=dag,
)
wait_for_migrations >> [stripe_task, zettle_task]
