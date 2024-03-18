from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.found_records_to_process import found_records_to_process

from plugins.operators.ensure_schema_exists import EnsurePostgresSchemaExistsOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.stripe_to_postgres_operator import StripeToPostgresOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.ensure_missing_columns_function import EnsureMissingColumnsPostgresFunctionOperator
from plugins.operators.ensure_datalake_table_view_exists import EnsurePostgresDatalakeTableViewExistsOperator
from plugins.operators.zettle_finance_to_postgres_operator import ZettleFinanceToPostgresOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator
from plugins.operators.zettle_purchases_to_postgres_operator import ZettlePurchasesToPostgresOperator

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

transient_schema_exists = EnsurePostgresSchemaExistsOperator(
    task_id="ensure_transient_schema_exists",
    schema="transient_data",
    postgres_conn_id="postgres_datalake_conn_id",
    dag=dag,
)
public_schema_exists = EnsurePostgresSchemaExistsOperator(
    task_id="ensure_public_schema_exists",
    schema="public",
    postgres_conn_id="postgres_datalake_conn_id",
    dag=dag,
)

ensure_missing_columns_function_exists = EnsureMissingColumnsPostgresFunctionOperator(
    task_id="ensure_missing_columns_function",
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    destination_schema="public",
    dag=dag,
)


stripe_balances_task = StripeToPostgresOperator(
    task_id="import_stripe_transactions_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    stripe_conn_id="stripe_conn_id",
    destination_schema="transient_data",
    destination_table="stripe__transactions",
    dag=dag,
)
task_id = "stripe_has_records_to_process"
stripe_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_stripe_transactions_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "stripe_ensure_datalake_table_exists"
stripe_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__transactions",
    destination_schema="public",
    destination_table="raw__stripe__transactions",
    dag=dag,
)

stripe_missing_columns_task_id = "stripe_ensure_public_columns_uptodate"
stripe_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=stripe_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="stripe__transactions",
    destination_table="raw__stripe__transactions",
    dag=dag,
)
task_id = "stripe_append_to_datalake"
stripe_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="stripe__transactions",
    destination_schema="public",
    destination_table="raw__stripe__transactions",
    dag=dag,
)

task_id = "stripe_ensure_datalake_table_view"
stripe_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__stripe__transactions",
    destination_schema="public",
    destination_table="stripe__transactions",
    prev_task_id=stripe_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    stripe_balances_task
    >> stripe_has_records_to_process
    >> stripe_ensure_datalake_table
    >> stripe_ensure_datalake_table_columns
    >> stripe_append_transient_table_data
    >> stripe_ensure_table_view_exists
)

zettle_task = ZettleFinanceToPostgresOperator(
    task_id="import_zettle_transactions_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    zettle_conn_id="zettle_conn_id",
    destination_schema="transient_data",
    destination_table="zettle__transactions",
    dag=dag,
)

task_id = "zettle_transactions_has_records_to_process"
zettle_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_zettle_transactions_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "zettle_transactions_ensure_datalake_table_exists"
zettle_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="zettle__transactions",
    destination_schema="public",
    destination_table="raw__zettle__transactions",
    dag=dag,
)

zettle_missing_columns_task_id = "zettle_transactions_ensure_public_columns_uptodate"
zettle_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=zettle_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="zettle__transactions",
    destination_table="raw__zettle__transactions",
    dag=dag,
)
task_id = "zettle_transactions_append_to_datalake"
zettle_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="zettle__transactions",
    destination_schema="public",
    destination_table="raw__zettle__transactions",
    dag=dag,
    delete_template="DELETE FROM {{ destination_schema }}.{{destination_table}} WHERE airflow_sync_ds='{{ ds }}'",
)

task_id = "zettle_transactions_ensure_datalake_table_view"
zettle_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__zettle__transactions",
    destination_schema="public",
    destination_table="zettle__transactions",
    prev_task_id=zettle_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    zettle_task
    >> zettle_has_records_to_process
    >> zettle_ensure_datalake_table
    >> zettle_ensure_datalake_table_columns
    >> zettle_append_transient_table_data
    >> zettle_ensure_table_view_exists
)

zettle_purchases_task = ZettlePurchasesToPostgresOperator(
    task_id="import_zettle_purchases_to_datalake",
    postgres_conn_id="postgres_datalake_conn_id",
    zettle_conn_id="zettle_conn_id",
    destination_schema="transient_data",
    destination_table="zettle__transactions",
    dag=dag,
)

task_id = "zettle_purchases_has_records_to_process"
zettle_purchases_has_records_to_process = ShortCircuitOperator(
    task_id=task_id,
    python_callable=found_records_to_process,
    op_kwargs={
        "parent_task_id": "import_zettle_purchases_to_datalake",
        "xcom_key": "documents_found",
    },
)

task_id = "zettle_purchases_ensure_datalake_table_exists"
zettle_purchases_ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="zettle__purchases",
    destination_schema="public",
    destination_table="raw__zettle__purchases",
    dag=dag,
)

zettle_purchases_missing_columns_task_id = "zettle_purchases_ensure_public_columns_uptodate"
zettle_purchases_ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
    task_id=zettle_purchases_missing_columns_task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_table="zettle__purchases",
    destination_table="raw__zettle__purchases",
    dag=dag,
)
task_id = "zettle_purchases_append_to_datalake"
zettle_purchases_append_transient_table_data = AppendTransientTableDataOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="transient_data",
    source_table="zettle__purchases",
    destination_schema="public",
    destination_table="raw__zettle__purchases",
    dag=dag,
    delete_template="DELETE FROM {{ destination_schema }}.{{destination_table}} WHERE airflow_sync_ds='{{ ds }}'",
)

task_id = "zettle_ensure_datalake_table_view"
zettle_purchases_ensure_table_view_exists = EnsurePostgresDatalakeTableViewExistsOperator(
    task_id=task_id,
    postgres_conn_id="postgres_datalake_conn_id",
    source_schema="public",
    source_table="raw__zettle__purchases",
    destination_schema="public",
    destination_table="zettle__purchases",
    prev_task_id=zettle_missing_columns_task_id,  # not append_transient data!!
    append_fields=["createdat", "updatedat", "airflow_sync_ds"],
    prepend_fields=["id"],
    dag=dag,
)

(
    zettle_purchases_task
    >> zettle_purchases_has_records_to_process
    >> zettle_purchases_ensure_datalake_table
    >> zettle_purchases_ensure_datalake_table_columns
    >> zettle_purchases_append_transient_table_data
    >> zettle_purchases_ensure_table_view_exists
)


(
    wait_for_migrations
    >> transient_schema_exists
    >> public_schema_exists
    >> ensure_missing_columns_function_exists
    >> [stripe_balances_task, zettle_task]
)
