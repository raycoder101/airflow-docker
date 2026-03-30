# dags/test_dag.py

from datetime import datetime
from airflow import DAG
from airflow.decorators import task

@task
def hello():
    return "hello world......"

@task
def transform(msg: str):
    return msg.upper()

with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    msg = hello()
    transform(msg)