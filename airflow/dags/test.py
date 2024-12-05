import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test-dag",
    description="테스트",
    start_date=pendulum.datetime(2024, 12, 1),
    schedule=None,
    catchup=False,
):
    start = EmptyOperator(task_id="start")

    @task(task_id="test")
    def test_python():
        for i in range(1, 10):
            print(i)

    end = EmptyOperator(task_id="end")

    start > test_python() > end
