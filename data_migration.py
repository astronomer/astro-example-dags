import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from migrations.migration_loader import load_migration_configs
from operators.mongoatlas_to_postgres import \
    MongoAtlasToPostgresViaDataframeOperator

# Load configurations from migrations directory
migrations_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")
migrations = load_migration_configs(migrations_dir)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 26),
}


@dag(
    schedule_interval=None,
    default_args=default_args,
    dag_id="data_migration_dag",
    start_date=datetime(2024, 1, 26),
)
def data_migration_dag():
    start_task = DummyOperator(task_id="start")

    @task
    def migration_task(config):
        migrate_op = MongoAtlasToPostgresViaDataframeOperator(
            mongo_jdbc_conn_id="mongo_jdbc_conn_id",
            postgres_conn_id="postgres_datalake_conn_id",
            destination_schema="transient_data",
            sql=config["sql"],
            preoperation=config["preoperation"],
            table=config["table"],
            task_id=config["task_id"],
        )
        # Execute the migration operation
        migrate_op.execute(context={})

    for config in migrations:
        migration_task(config)

    return start_task


dag = data_migration_dag()
