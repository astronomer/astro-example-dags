from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from operators.mongoatlas_to_postgres import \
    MongoAtlasToPostgresViaDataframeOperator

from data_migrations.migration_loader import load_migration_configs

# Now load the migrations
migrations = load_migration_configs("migrations")

print(migrations)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 26),
}


@dag(
    schedule_interval=None,
    default_args=default_args,
    dag_id="data_migration_dag",
    template_searchpath="/usr/local/airflow/dags",
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

    migration_tasks = [migration_task(config) for config in migrations]

    start_task >> migration_tasks

    return start_task


dag = data_migration_dag()
