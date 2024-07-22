import logging
import sys
from airflow import DAG
from common.common_func import regist

import pendulum

from airflow.decorators import dag, task


with DAG(
    dag_id = 'dags_python_with_op_args',
    schedule = None,
    start_date = pendulum.datetime(2024, 7, 1, tz="Asia/Seoul"),
    catchup = False,
    tags=["practice"]
) as dag:

    py_task_1 = regist('minding', 'man', 'kr', 'seoul')
        # [END howto_operator_python]