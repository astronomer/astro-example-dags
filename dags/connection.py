"""Example DAG demonstrating the usage of the JdbcOperator."""

import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from operators.mongoatlas_to_postgres import \
    MongoAtlasToPostgresViaDataframeOperator

task_logger = logging.getLogger("airflow.task")
task_logger.debug("CONNECT_START!")


@dag(
    dag_id="test_JDBC_operator",
    schedule_interval="0 0 * * *",
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
)
def run_dag():
    # MongoAtlasToPostgresViaDataframeOperator(
    #     task_id="migrate__partners__to_postgres",
    #     mongo_jdbc_conn_id="AtlasSQL",
    #     postgres_conn_id="PostgresDatalake",
    #     sql="SELECT {cols} FROM FLATTEN(partners) LIMIT {limit} OFFSET {offset}",
    #     table="partners",
    #     destination_schema="transient_data",
    # )
    MongoAtlasToPostgresViaDataframeOperator(
        task_id="migrate__experiments__to_postgres",
        mongo_jdbc_conn_id="AtlasSQL",
        postgres_conn_id="PostgresDatalake",
        sql="SELECT {cols} FROM FLATTEN(experiments) LIMIT {limit} OFFSET {offset}",
        table="experiments",
        destination_schema="transient_data",
    )
    # MongoAtlasToPostgresViaDataframeOperator(
    #     task_id="migrate__transactions__to_postgres",
    #     mongo_jdbc_conn_id="AtlasSQL",
    #     postgres_conn_id="PostgresDatalake",
    #     sql="SELECT {cols} FROM FLATTEN(transactions) LIMIT {limit} OFFSET {offset}",
    #     table="transactions",
    #     destination_schema="transient_data",
    # )


run_dag()
