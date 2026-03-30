# tests/test_dag.py

from airflow.models import DagBag

def test_dag_loaded():
    dag_bag = DagBag(include_examples=False)
    assert "test_dag" in dag_bag.dags

def test_dag_structure():
    dag = DagBag(include_examples=False).get_dag("test_dag")

    assert len(dag.tasks) == 2
    assert set(t.task_id for t in dag.tasks) == {"hello", "transform"}