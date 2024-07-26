from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator): # 클래스 상속
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt') # 템플릿 문법 사용 가능한 파라미터 설정

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs): # 4개의 파라미터를 User에게 받을 예정
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr' # 커넥션 ID를 미리 지정 (input으로 받지 않고 이것만 사용)
        self.path = path # 파일을 저장할 경로
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json' + dataset_nm
        self.base_dt = base_dt

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id) # BaseHook의 get_connection을 이용해 Webserver의 커넥션 정보를 꺼내올 수 있음
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000

        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row) # 아래 선언한 _call_api 함수를 통해 api 호출
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000: # 1000개씩 호출하는데, 1000개 이하라면 더 이상 긁어올 데이터가 없다는 것을 뜻함
                break
            else:
                start_row = end_row + 1
                end_row += 1000 # 1000개 더 호출

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)


    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json
        
        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }
        
        request_url = f'{base_url}/{start_row}/{end_row}/' # 가져올 데이터 수에 대한 링크 추가
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers) # 위 링크를 토대로 get 명령 호출
        contents = json.loads(response.text) # 위에서 받은 데이터를 json으로 load

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row') # 실질적인 데이터가 있는 row 키의 value값을 불러옴
        row_df = pd.DataFrame(row_data) # 해당 데이터를 dataframe으로 저장

        return row_df