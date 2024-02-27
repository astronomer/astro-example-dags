import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from plugins.operators.drop_table import DropPostgresTableOperator
from plugins.operators.analyze_table import RefreshPostgresTableStatisticsOperator
from data_migrations.aggregation_loader import load_aggregation_configs
from plugins.operators.mongodb_to_postgres import MongoDBToPostgresViaDataframeOperator
from plugins.operators.ensure_schema_exists import EnsurePostgresSchemaExistsOperator
from plugins.operators.ensure_missing_columns import EnsureMissingPostgresColumnsOperator
from plugins.operators.ensure_datalake_table_exists import EnsurePostgresDatalakeTableExistsOperator
from plugins.operators.ensure_missing_columns_function import EnsureMissingColumnsPostgresFunctionOperator
from plugins.operators.append_transient_table_data_operator import AppendTransientTableDataOperator

# Now load the migrations
migrations = load_aggregation_configs("aggregations")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
}


dag = DAG(
    "data_aggregation_dag",
    schedule_interval=None,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/dags",
)

start_task = DummyOperator(task_id="start", dag=dag)
generated_schemas_path = "../include/generatedSchemas/"
generated_schemas_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), generated_schemas_path)


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
migration_tasks = []
for config in migrations:
    schema_path = os.path.join(generated_schemas_abspath, config["jsonschema"])

    task_id = f"{config['task_name']}_drop_transient_table_if_exists"
    drop_transient_table = DropPostgresTableOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="transient_data",
        table=config["destination_table"],
        dag=dag,
    )

    task_id = f"{config['task_name']}_migrate_to_postgres"
    mongo_to_postgres = MongoDBToPostgresViaDataframeOperator(
        task_id=task_id,
        mongo_conn_id="mongo_db_conn_id",
        postgres_conn_id="postgres_datalake_conn_id",
        preoperation=config["preoperation"],
        aggregation_query=config["aggregation_query"],
        source_collection=config["source_collection"],
        source_database="harper-production",
        jsonschema=schema_path,
        destination_schema="transient_data",
        destination_table=config["destination_table"],
        unwind=config.get("unwind"),
        preserve_fields=config.get("preserve_fields", []),
        discard_fields=config.get("discard_fields", []),
        convert_fields=config.get("convert_fields", []),
        dag=dag,
    )

    task_id = f"{config['task_name']}_refresh_transient_table_stats"
    refresh_transient_table = RefreshPostgresTableStatisticsOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="transient_data",
        table=config["destination_table"],
        dag=dag,
    )

    task_id = f"{config['task_name']}_ensure_public_schema_exists"
    ensure_datalake_table = EnsurePostgresDatalakeTableExistsOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        source_schema="transient_data",
        source_table=config["destination_table"],
        destination_schema="public",
        destination_table=config["destination_table"],
        dag=dag,
    )

    task_id = f"{config['task_name']}_refresh_datalake_table_stats"
    refresh_datalake_table = RefreshPostgresTableStatisticsOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="public",
        table=config["destination_table"],
        dag=dag,
    )

    task_id = f"{config['task_name']}_ensure_public_columns_uptodate"
    ensure_datalake_table_columns = EnsureMissingPostgresColumnsOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        table=config["destination_table"],
        dag=dag,
    )
    task_id = f"{config['task_name']}_append_to_datalake"
    append_transient_table_data = AppendTransientTableDataOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        source_schema="transient_data",
        table=config["destination_table"],
        destination_schema="public",
        dag=dag,
    )
    (
        drop_transient_table
        >> mongo_to_postgres
        >> refresh_transient_table
        >> ensure_datalake_table
        >> refresh_datalake_table
        >> ensure_datalake_table_columns
        >> append_transient_table_data
    )
    migration_tasks.append(mongo_to_postgres)

(
    start_task
    >> transient_schema_exists
    >> public_schema_exists
    >> ensure_missing_columns_function_exists
    >> migration_tasks
)
