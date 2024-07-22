from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1", # 첫 번째 토요일 0시 10분마다
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["practice"]
) as dag:
    
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command = 'echo "end date is {{ data_interval_end }}"'
    )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env = {'START_DATE': '{{ data_interval_start | ds}}', # 일종의 변수 정의 개념
               'END_DATE': '{{ data_interval_end | ds}}'}, # YYYY-MM-DD 형식으로 출력됨
        bash_command = 'echo "Start date is $START_DATE " &&'
                        'echo "End date is $END_DATE"'
    )

    bash_t1 >> bash_t2