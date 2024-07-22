from airflow.decorators import task

def get_sftp():
    print('작업을 시작합니다.')

@task(task_id = 'regist')
def regist(name, sex, *args):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션: {args}')