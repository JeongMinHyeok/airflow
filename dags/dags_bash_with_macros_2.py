from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_2",
    schedule="10 0 * * 6#2", # 두 번째 토요일 0시 10분마다
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["practice"]
) as dag:
    # START_DATE: 2주 전 월요일, END_DATE: 2주 전 토요일
    bash_task_2 = BashOperator(
        task_id = 'bash_task_2',
        env = {'START_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelat.relativedelat(days=19)) | ds }}', # 배치일로부터 19일 전 = 2주 전 월요일
               'END_DATE' : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelat.relativedelat(days=14)) | ds }}'} # 배치일로부터 14일 전 = 2주 전 토요일
        bash_command = 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )