from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 3, 10, tz="Asia/Seoul"),
    tags=["practice"],
    catchup=False
) as dag:
    
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator( # 첫 번째 방법: templates 변수를 직접 부르는 방법
        task_id='python_t1',
        python_callable=python_function1,
        op_kwargs={'start_date':'{{data_interval_start | ds}}', 'end_date':'{{data_interval_end | ds}}'}
    )

    @task(task_id='python_t2')
    def python_function2(**kwargs): # 두 번째 방법: kwargs 딕셔너리 내 자동 제공되는 templates 변수들을 불러오기
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts:' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))
        print('data_interval_end:' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))


    python_t1 >> python_function2()