from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id = 'dags_python_show_templates',
    schedule = "30 9 * * *", # daily
    start_date = pendulum.datetime(2024, 7, 10, tz="Asia/Seoul"),
    catchup = True, # 7월 10일부터 현재 날짜까지를 모두 수행
    tags=["practice"]
) as dag:
    
    @task(task_id = 'python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()