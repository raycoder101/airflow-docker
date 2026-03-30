# tests/test_tasks.py

from dags.test_dag import hello, transform

def test_hello():
    assert hello().function() == "hello"

def test_transform():
    assert transform().function("abc") == "ABC"