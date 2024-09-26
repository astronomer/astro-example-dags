"""
this is for testing
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

#Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["myfirstdag"],
)
start = print("I am in start")
end = print("i am in end")

start >> end
