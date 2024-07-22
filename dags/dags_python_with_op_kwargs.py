from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist2
import pendulum

with DAG(
    dag_id = 'dags_python_with_op_kwargs',
    schedule = None,
    start_date = pendulum.datetime(2024, 7, 1, tz="Asia/Seoul"),
    catchup = False,
    tags=["practice"]
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id = 'regist2_t1',
        python_callable = regist2,
        op_args = ['minding', 'man', 'kr', 'seoul'],
        op_kwargs = {'email':'tlsfk48@gmail.com', 'phone':'01011112222'}
    )

    regist2_t1