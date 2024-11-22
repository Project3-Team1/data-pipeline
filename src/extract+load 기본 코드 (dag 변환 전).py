import os
import json
import logging
import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DataError
import re

logging.basicConfig(filename="GET_DATA_EXCUTE.log", level=logging.INFO)

USERNAME, PASSWORD = 'root', 'password'
engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@localhost:3306/DE4_PROJECT3')
# df.to_sql(name='bronze_weather_data', con=engine, if_exists='replace', index=False)

logging.info('READ_META_DATA')

metadata = pd.read_excel('서울시 주요 116장소명 목록.xlsx')
API_KEY = '694f44565773747936374a62587a66'


logging.info('INIT SUCCESS')


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



for area_id in metadata['AREA_CD'].tolist():
    logging.info(f'READ_DATA : {area_id}')
    url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/json/citydata/1/5/{area_id}'
    create_at = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    
    data = requests.get(url)
    
    json_data = json.loads(data.text)['CITYDATA']
    
    # make_DataFrame
    logging.info(f'MAKE_DATAFRAME : {area_id}')

    df_charger = df_append_metadata(pd.json_normalize(json_data.get('CHARGER_STTS',[]) or []), area_id, create_at)
    df_event = df_append_metadata(pd.json_normalize(json_data.get('EVENT_STTS',[]) or []), area_id, create_at)
    df_live_cmrcl = df_append_metadata(pd.json_normalize(json_data.get('LIVE_CMRCL_STTS',[]) or []), area_id, create_at)
    df_live_ppltn = df_append_metadata(pd.json_normalize(json_data.get('LIVE_PPLTN_STTS',[]) or []), area_id, create_at)
    df_prk_stts = df_append_metadata(pd.json_normalize(json_data.get('PRK_STTS',[]) or []), area_id, create_at)
    df_road_traffic = df_append_metadata(pd.json_normalize(json_data.get('ROAD_TRAFFIC_STTS',[]) or []), area_id, create_at)
    df_sbike = df_append_metadata(pd.json_normalize(json_data.get('SBIKE_STTS',[]) or []), area_id, create_at)
    df_sub_stts = df_append_metadata(pd.json_normalize(json_data.get('SUB_STTS', []) or []), area_id, create_at)
    df_weather = df_append_metadata(pd.json_normalize(json_data.get('WEATHER_STTS',[]) or []), area_id, create_at)
    

    logging.info(f'APPEND DATABASE : {area_id}')
        
    df_load_data(df_charger, engine = engine, table_name = 'BRONZE_CHARGER_STTS')
    df_load_data(df_event, engine = engine, table_name = 'BRONZE_EVENT_STTS')
    df_load_data(df_live_cmrcl, engine = engine, table_name = 'BRONZE_LIVE_CMRCL_STTS')
    df_load_data(df_live_ppltn, engine = engine, table_name = 'BRONZE_LIVE_PPLTN_STTS')
    df_load_data(df_prk_stts, engine = engine, table_name = 'BRONZE_PRK_STTS')
    df_load_data(df_road_traffic, engine = engine, table_name = 'BRONZE_ROAD_TRAFFIC_STTS')
    df_load_data(df_sbike, engine = engine, table_name = 'BRONZE_SBIKE_STTS')
    df_load_data(df_sub_stts, engine = engine, table_name = 'BRONZE_SUB_STTS')
    df_load_data(df_weather, engine = engine, table_name = 'BRONZE_WEATHER_STTS')
    
        


