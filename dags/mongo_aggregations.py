import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from operators.mongodb_to_postgres import MongoDBToPostgresViaDataframeOperator
from data_migrations.aggregation_loader import load_aggregation_configs

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


migration_tasks = []
for config in migrations:
    schema_path = os.path.join(generated_schemas_abspath, config["jsonschema"])

    task = MongoDBToPostgresViaDataframeOperator(
        task_id=config["task_id"],
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
        preserve_fields=config.get("preserve_fields"),
        dag=dag,
    )
    migration_tasks.append(task)

    # Set dependency for each task
    start_task >> task
