

import logging

from sqlalchemy import inspect, text
from sqlalchemy.exc import DataError
import re


def df_append_metadata(df, area_id, create_at):
    df['AREA_CD'] = area_id
    df['CreateAt'] = create_at
    
    # object 형태 변환 (dict > str)
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(lambda x: str(x))
            
    return df


def df_load_data(df, engine, table_name):
    # 테이블 스키마 확인 및 동적 업데이트
    with engine.connect() as conn:
        inspector = inspect(conn)

        # 테이블 존재 여부 확인
        if table_name in inspector.get_table_names():
            db_columns = [col["name"] for col in inspector.get_columns(table_name)]
        
            # 데이터프레임에만 있는 열 찾기
            extra_columns = [col for col in df.columns if col not in db_columns]
        
            # 테이블에 누락된 열 동적으로 추가
            for col in extra_columns:
                alter_query = text(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")
                conn.execute(alter_query)
                logging.info(f'{col} 열이 {table_name} 테이블에 추가되었습니다')
        else:
            logging.info(f"테이블 {table_name}이(가) 존재하지 않아 새로 생성됩니다.")
            db_columns = []

        # 데이터 적재
        try:
            df.to_sql(table_name, conn, if_exists="append", index=False)
        except DataError as e:
            # 에러 메시지에서 열 이름 추출
            error_msg = str(e.orig)  # pymysql 에러 메시지
            column_name = re.search(r"Data too long for column '(.*?)'", error_msg).group(1)
            logging.info(f"{table_name} 테이블, '{column_name}'열 의 크기를 MEDIUMTEXT로 변경 (적재 최대 크기 초과)")
              
            alter_query = f"""
                ALTER TABLE {table_name} 
                MODIFY COLUMN {column_name} MEDIUMTEXT;
                """
            conn.execute(alter_query)

            df.to_sql(table_name, conn, if_exists='append', index=False)