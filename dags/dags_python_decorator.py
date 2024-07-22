import logging
import sys
from airflow import DAG

import pendulum

from airflow.decorators import dag, task


with DAG(
    dag_id = 'dags_python_decorator',
    schedule = None,
    start_date = pendulum.datetime(2024, 7, 1, tz="Asia/Seoul"),
    catchup = False,
    tags=["practice"]
) as dag:

    @task(task_id = 'py_task_1')
    def print_context(some_input):
        print(some_input)

    py_task_1 = print_context('task decorator 실행')
        # [END howto_operator_python]