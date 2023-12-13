import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator

from scripts import common

@dag(
    dag_id="s3_to_postgres",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30),
    max_active_runs=10,
    tags=["s3_copy","common"],
    params={
        "s3_bucket": Param("", type="string"),
        "s3_key": Param("", type="string"),
    },
    default_args={
        "retries": 2,
    },

)
def copy_data():

    @task(multiple_outputs=True)
    def parse_parameters(bucket, key):
        return common.parse_parameters(bucket, key)
    
    parsed_key = parse_parameters("{{ params.s3_bucket }}","{{ params.s3_key }}")

    add_metadata = S3FileTransformOperator(
        task_id="add_metadata",
        source_s3_key=parsed_key["full_path"],
        dest_s3_key=parsed_key["modified_full_path"],
        transform_script='scripts/add_metadata.py',
        script_args=[str(parsed_key)],
        replace=True,
        source_aws_conn_id='aws_account',
        dest_aws_conn_id='aws_account'
    )
    print('modified',str(parsed_key["modified_full_path"]))
    s3_to_postgres = S3ToSqlOperator(
        task_id="copy_data_from_s3_to_postgres",
        s3_bucket=parsed_key['bucket'],
        s3_key=parsed_key["modified_key"],
        schema=parsed_key["schema"],
        table=parsed_key["table"],
        parser=common.parse_csv_to_generator,
        sql_conn_id='data_postgres',
        aws_conn_id='aws_account'
    )

    add_metadata >> s3_to_postgres

copy_data()
