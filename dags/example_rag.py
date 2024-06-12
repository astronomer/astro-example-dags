"""
## 
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from pendulum import datetime
from tabulate import tabulate
import pandas as pd
import duckdb
import logging
import os

# use the Airflow task logger to log information to the task logs
t_log = logging.getLogger("airflow.task")

# define variables used in a DAG in the DAG code or as environment variables for your whole Airflow instance
# avoid hardcoding values in the DAG code
_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "embeddings_table")
_DUCKDB_TABLE_URI = f"duckdb://{_DUCKDB_INSTANCE_NAME}/{_DUCKDB_TABLE_NAME}"


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 5, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example", "ETL"],
)
def example_rag():

    @task
    def proprietary_information():
        faq = ["apple", "walking", "planet", "light", "happiness"]

        return faq

    @task
    def create_embeddings(info):
        import requests

        model_id = "sentence-transformers/all-MiniLM-L6-v2"
        api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_id}"

        def query(texts):
            response = requests.post(
                api_url, json={"inputs": texts, "options": {"wait_for_model": True}}
            )
            return response.json()

        words_and_embeddings = {"word": info, "embeddings": query(info)}
        return words_and_embeddings

    @task
    def create_vector_table(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
        embeddings: list = None,
    ):
        """ """

        print(embeddings)

        cursor = duckdb.connect(duckdb_instance_name)
        cursor.execute("INSTALL vss;")
        cursor.execute("LOAD vss;")
        cursor.execute("SET hnsw_enable_experimental_persistence = true;")

        cursor.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} (text STRING, vec FLOAT[3]);
            INSERT INTO {table_name} (text, vec)
                SELECT 'entry' || CAST(a AS STRING), [a*0.1, b*0.1, c*0.1] 
                FROM range(1, 10) ra(a), range(1, 10) rb(b), range(1, 10) rc(c);
            CREATE INDEX my_hnsw_index ON {table_name} USING HNSW (vec);
            """
        )

        top_3 = cursor.execute(
            f"""
            SELECT * FROM {table_name} 
            ORDER BY array_distance(vec, [1, 2, 3]::FLOAT[3]) 
            LIMIT 3;
            """
        )

        print(top_3.fetchall())

    chain(create_vector_table(embeddings=create_embeddings(proprietary_information())))


example_rag()
