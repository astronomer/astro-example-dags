import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": datetime(2023,8,10,17,15)}

dag = DAG(
    dag_id="BW_test_conn", default_args=args, schedule_interval=None
)

query1 = [
    """select 1;""",
    """show tables in database CLEARAIR_ZENDESK;""",
]


def count1(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("select count(*) from DEVELOPMENT_DW.DBT_OATANDA.dimdate")
    logging.info("Number of rows in the table DBT_OATANDA.dimdate  - %s", result[0])


with dag:
    # query1_exec = SnowflakeOperator(
    #     task_id="snowflake_task",
    #     sql=query1,
    #     snowflake_conn_id="snowflake_conn",
    # )

    test_count = PythonOperator(task_id="test_count", python_callable=count1)
# query1_exec >> count_query
test_count