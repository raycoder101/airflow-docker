from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="simple_dag",
    schedule=timedelta(minutes=60),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
)
def sample_pipeline():
    @task()
    def extract(): return {"data": [1, 2, 3]}
    
    @task()
    def transform(data: dict): return [x * 2 for x in data["data"]]
    
    @task()
    def load(data: list): print(f"Loaded: {data}")

    load(transform(extract()))

sample_pipeline()