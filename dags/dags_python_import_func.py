from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp # airflow는 기본적으로 /plugins 폴더까지 path로 인식하기 때문에 그 아래 경로로 import 해줘야 함

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *", # 매일 6시 30분 마다
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    task_get_sftp = PythonOperator(
        task_id = 'task_get_sftp',
        python_callable=get_sftp
    )

    task_get_sftp