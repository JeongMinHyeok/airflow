from operators.seoul_api_to_csv import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024,7,1, tz='Asia/Seoul'),
    tags = ['practice'],
    catchup=False
) as dag:
    '''서울시 10분 강우량 동향'''
    seoul_rain_fall_10m = SeoulApiToCsvOperator(
        task_id = 'seoul_rain_fall_10m',
        dataset_nm='ListRainfallService',
        path='/home/minding/airflow/files/seoul_rainfall_10m/{{data_interval_end.in_timezone("Asia/Seoul) | ds_nodash}}',
        file_name='Seoul_Rainfall_10m.csv'
    )

    '''서울시 일일 강수량 현향'''
    ts_rainfall_data = SeoulApiToCsvOperator(
        task_id = 'ts_rainfall_data',
        dataset_nm='tsRainfallData',
        path='/home/minding/airflow/files/ts_rainfall_data/{{data_interval_end.in_timezone("Asia/Seoul) | ds_nodash}}',
        file_name='ts_rainfall_data.csv'
    )

    seoul_rain_fall_10m >> ts_rainfall_data

