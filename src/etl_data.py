
import json
import logging
import requests
from sqlalchemy import create_engine

from collections import defaultdict
import pandas as pd
import datetime

from src.utils.utils import df_append_metadata, df_load_data



def extract_area_info(metadata, api_address):

    logging.basicConfig(filename="GET_DATA_EXCUTE.log", level=logging.INFO)
    

    raw_area_info = defaultdict()
    for area_id in metadata:

        logging.info(f'READ_DATA : {area_id}')
        
        url = f'http://{api_address}/{area_id}'
        create_at = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        
        data = requests.get(url)
        
        try:
            raw_area_info[area_id] = json.loads(data.text.encode('utf-8'))['CITYDATA']
        except:
            logging.error(data.text)
    
    return raw_area_info, create_at
    

def transform_area_info(raw_area_info, create_at):
    area_info = defaultdict(dict)
    for area_id in raw_area_info:
        # make_DataFrame
        logging.info(f'MAKE_DATAFRAME : {area_id}')

        area_info[area_id]['CHARGER_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('CHARGER_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['EVENT_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('EVENT_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['LIVE_CMRCL_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('LIVE_CMRCL_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['LIVE_PPLTN_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('LIVE_PPLTN_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['PRK_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('PRK_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['ROAD_TRAFFIC_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('ROAD_TRAFFIC_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['SBIKE_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('SBIKE_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['SUB_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('SUB_STTS',[]) or []), area_id, create_at)
        area_info[area_id]['WEATHER_STTS'] = df_append_metadata(pd.json_normalize(raw_area_info[area_id].get('WEATHER_STTS',[]) or []), area_id, create_at)
    
    return area_info


def load_area_info(area_info, db_connection_info):
    engine = create_engine(f'mysql+pymysql://{db_connection_info["user"]}:{db_connection_info["password"]}@{db_connection_info["host"]}:{db_connection_info["port"]}/{db_connection_info["schema"]}')
    # df.to_sql(name='bronze_weather_data', con=engine, if_exists='replace', index=False)
    logging.info('DB CONNECTION SUCCESS')

    logging.info(pd.__version__)
    
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
    
    engine.dispose()
    logging.info('DB CONNECTION CLOSED')


