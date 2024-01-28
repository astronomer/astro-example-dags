from airflow import DAG
from airflow.providers.snowflake.transfers.snowflake_to_snowflake import SnowflakeToSnowflakeOperator
from datetime import datetime, timedelta

# Define your DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
dag = DAG(
    'snowflake_copy_data',
    default_args=default_args,
    schedule_interval='@daily',
)

# Define the SnowflakeOperator to copy data
copy_data_task = SnowflakeToSnowflakeOperator(
    task_id='copy_customer_data',
    sql='INSERT INTO customer_transformed SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;',
    snowflake_conn_id='your_snowflake_conn',  # Specify your Snowflake connection ID
    warehouse='your_warehouse',  # Specify your Snowflake warehouse
    database='your_database',  # Specify your Snowflake database
    schema='your_schema',  # Specify your Snowflake schema
    autocommit=True,
    dag=dag,
)

# Set the task dependencies (if any)
# For example, you might have other tasks that need to run before this one

# Add the task to the DAG
dag >> copy_data_task
