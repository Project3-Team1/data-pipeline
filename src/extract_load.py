
import json
import logging
import requests
from collections import defaultdict
import pandas as pd
import datetime
from sqlalchemy import create_engine, inspect, text
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
            # create_query = ""
            # conn.execute(create_query)
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





def extract_load_area_info(metadata, API_KEY):

    logging.basicConfig(filename="GET_DATA_EXCUTE.log", level=logging.INFO)
    

    area_info_raw = defaultdict()
    for area_id in metadata:

        logging.info(f'READ_DATA : {area_id}')
        
        url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/json/citydata/1/5/{area_id}'
        create_at = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        
        data = requests.get(url)
        
        area_info_raw[area_id] = json.loads(data.text.encode('utf-8'))['CITYDATA']
    

    area_info = defaultdict(dict)
    for area_id in area_info_raw:
        # make_DataFrame
        logging.info(f'MAKE_DATAFRAME : {area_id}')

        area_info[area_id]['CHARGER_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('CHARGER_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['EVENT_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('EVENT_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['LIVE_CMRCL_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('LIVE_CMRCL_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['LIVE_PPLTN_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('LIVE_PPLTN_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['PRK_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('PRK_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['ROAD_TRAFFIC_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('ROAD_TRAFFIC_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['SBIKE_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('SBIKE_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['SUB_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('SUB_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['WEATHER_STTS'] = df_append_metadata(pd.json_normalize(area_info_raw[area_id].get('WEATHER_STTS',[]) or []), area_id, create_at)
    

    USERNAME, PASSWORD = 'admin', 'admin'
    engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@localhost:3306/DE4_PROJECT3')
    # df.to_sql(name='bronze_weather_data', con=engine, if_exists='replace', index=False)
    logging.info('DB CONNECTION SUCCESS')
    
    for area_id in area_info:

        logging.info(f'APPEND DATABASE : {area_id}')
            
        df_load_data(area_info[area_id]['CHARGER_STTS'], engine = engine, table_name = 'BRONZE_CHARGER_STTS')
        df_load_data(area_info[area_id]['EVENT_STTS'], engine = engine, table_name = 'BRONZE_EVENT_STTS')
        df_load_data(area_info[area_id]['LIVE_CMRCL_STTS'], engine = engine, table_name = 'BRONZE_LIVE_CMRCL_STTS')
        df_load_data(area_info[area_id]['LIVE_PPLTN_STTS'], engine = engine, table_name = 'BRONZE_LIVE_PPLTN_STTS')
        df_load_data(area_info[area_id]['PRK_STTS'], engine = engine, table_name = 'BRONZE_PRK_STTS')
        df_load_data(area_info[area_id]['ROAD_TRAFFIC_STTS'], engine = engine, table_name = 'BRONZE_ROAD_TRAFFIC_STTS')
        df_load_data(area_info[area_id]['SBIKE_STTS'], engine = engine, table_name = 'BRONZE_SBIKE_STTS')
        df_load_data(area_info[area_id]['SUB_STTS'], engine = engine, table_name = 'BRONZE_SUB_STTS')
        df_load_data(area_info[area_id]['WEATHER_STTS'], engine = engine, table_name = 'BRONZE_WEATHER_STTS')
    
        


