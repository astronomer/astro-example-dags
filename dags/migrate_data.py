from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from data_migrations.migration_loader import load_migration_configs
from operators.mongoatlas_to_postgres import MongoAtlasToPostgresViaDataframeOperator

# Now load the migrations
migrations = load_migration_configs("migrations")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
}


dag = DAG(
    "data_migration_dag",
    schedule_interval=None,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/dags",
)

start_task = DummyOperator(task_id="start", dag=dag)

migration_tasks = []
for config in migrations:
    unwind = config.get("unwind")
    unwind_prefix = config.get("unwind_prefix")
    task = MongoAtlasToPostgresViaDataframeOperator(
        task_id=config["task_id"],
        mongo_jdbc_conn_id="mongo_jdbc_conn_id",
        postgres_conn_id="postgres_datalake_conn_id",
        destination_schema="transient_data",
        sql=config["sql"],
        preoperation=config["preoperation"],
        table=config["table"],
        unwind_prefix=f"{unwind}{unwind_prefix}",
        dag=dag,
    )
    migration_tasks.append(task)

    # Set dependency for each task
    start_task >> task
