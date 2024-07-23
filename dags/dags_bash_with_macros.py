from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_1",
    schedule="10 0 L * *", # 매 월 말일 0시 10분
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["practice"]
) as dag:
    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task_1 = BashOperator(
        task_id = 'bash_task_1',
        env = {'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}', # .in_timezone() : ()안의 해당하는 시간대로 출력해줌 (기본 설정 = UTC)
               'END_DATE' : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelat.relativedelat(days=1)) | ds }}'} # 수행일로부터 하루 전(1일 빼기)
        bash_command = 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )
    