from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *", # 매월 1일 8시
    start_date=pendulum.datetime(2024, 7, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='tlsfkaus4862@naver.com', # 수신자
        # cc= '참조'
        subject='Airflow 성공메일', # 제목
        html_content='Airflow 작업이 완료되었습니다' # 내용
    )