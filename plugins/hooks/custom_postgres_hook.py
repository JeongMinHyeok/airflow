from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook): # BaseHook 상속

    def __init__(self, postgres_conn_id, **kwargs): # 생성자에서는 postgres에 대한 connection id만 인자값으로 받음
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self): # get_conn() 함수 재정의: get_connection() 메서드 이용해 정보 받아옴
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        # 위에서 받아온 정보를 토대로 psycopg2 이용해 DB와의 연결 세션 생성
        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn # 해당 세션을 return

    # bulk_load() 함수 생성: 테이블 이름, 파일 이름, 구분형식, 헤더 포함여부, 기존 데이터 삭제여부를 인자로 받음
    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        self.get_conn()
        header = 0 if is_header else None                       # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append'       # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:                             
            try:
                # string 문자열 칼럼이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n','')      # 줄넘김 및 ^M 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue 
                
        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri) # sqlalchemy 기능
        file_df.to_sql(name=table_name,
                            con=engine,
                            schema='public',
                            if_exists=if_exists,
                            index=False
                        )