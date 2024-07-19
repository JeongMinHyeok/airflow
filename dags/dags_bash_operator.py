from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_bash_operator", # Airflow UI에서 보이는 이름(py 파일 이름과 관계없으나 동일하게 하는 것이 좋음)
    schedule="0 0 * * *", # 분 / 시 / 일 / 월 / 요일
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False, # 현재 시점을 기준으로 start_date부터 누락된 workflow의 실행 여부 (True: 누락된 부분 모두 실행)
    # dagrun_timeout=datetime.timedelta(minutes=60), # n분 이상 실행하고 있을 시 실패로 판단
    tags=["practice"], # UI의 DAG 아래에 있는 태그 설정
    # params={"example_key": "example_value"}, # dag 설정 아래에 공통적인 인자가 있을 시
) as dag: # 실제 Task를 아래에 작성
    bash_t1 = BashOperator(
        task_id="bash_t1", # UI에 나타나는 Task의 이름
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2", # UI에 나타나는 Task의 이름
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2 # task의 실행 순서 명시