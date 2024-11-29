

import logging

from sqlalchemy import inspect, text, engine
from sqlalchemy.exc import DataError
import pandas as pd
import re


def df_append_metadata(df:pd.core.frame.DataFrame, area_id:str, create_at:str) -> pd.core.frame.DataFrame:
    """
    add metadata info
        - input: dataframe, area_id, timestamp
        - output: dataframe with metadata
    """

    df['AREA_CD'] = area_id
    df['CreateAt'] = create_at
    
    # object 형태 변환 (dict > str)
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(lambda x: str(x))
            
    return df


def df_load_data(df:pd.core.frame.DataFrame, engine:engine.base.Engine, table_name:str):
    """
    load data from dataframe to DB table
        - input: dataframe, engine (DB connection), table name 
    """

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


def remove_duplicate_data(engine:engine.base.Engine):
    """
    remove duplicate data for incremental update
        - input: engine (DB connection)
    """

    with engine.connect() as conn:
        # CHARGER_STTS
        logging.info(f'REMOVE DUPLICATE DATA : CHARGER_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_CHARGER_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_CHARGER_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_CHARGER_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_CHARGER_STTS AS 
                (SELECT STAT_NM, STAT_ID, STAT_ADDR, STAT_X, STAT_Y, STAT_USETIME, 
                        STAT_PARKPAY, STAT_LIMITYN, STAT_LIMITDETAIL, STAT_KINDDETAIL, 
                        CHARGER_DETAILS, AREA_CD, CreateAt 
                FROM
                    (SELECT *, 
                            DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date, 
                            ROW_NUMBER() OVER(PARTITION BY STAT_ID, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq 
                    FROM DE4_PROJECT3.TEMP_BRONZE_CHARGER_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_CHARGER_STTS""")

        # EVENT_STTS
        logging.info(f'REMOVE DUPLICATE DATA : EVENT_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_EVENT_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_EVENT_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_EVENT_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_EVENT_STTS AS 
                (SELECT EVENT_NM, EVENT_PERIOD, EVENT_PLACE, EVENT_X, EVENT_Y, PAY_YN, THUMBNAIL, 
                    URL, EVENT_ETC_DETAIL, AREA_CD, CreateAt
                FROM 
                    (SELECT *,
                            DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date,
                            ROW_NUMBER() OVER (PARTITION BY EVENT_NM, EVENT_PLACE, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq
                    FROM DE4_PROJECT3.TEMP_BRONZE_EVENT_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_EVENT_STTS""")

        # LIVE_CMRCL_STTS
        logging.info(f'REMOVE DUPLICATE DATA : LIVE_CMRCL_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_LIVE_CMRCL_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_LIVE_CMRCL_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_LIVE_CMRCL_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_LIVE_CMRCL_STTS AS 
                (SELECT AREA_CMRCL_LVL, AREA_SH_PAYMENT_CNT, AREA_SH_PAYMENT_AMT_MIN, AREA_SH_PAYMENT_AMT_MAX, 
                        CMRCL_RSB, CMRCL_MALE_RATE, CMRCL_FEMALE_RATE, CMRCL_10_RATE, CMRCL_20_RATE, CMRCL_30_RATE, 
                        CMRCL_40_RATE, CMRCL_50_RATE, CMRCL_60_RATE, CMRCL_PERSONAL_RATE, CMRCL_CORPORATION_RATE, 
                        CMRCL_TIME, AREA_CD, CreateAt
            FROM (
                SELECT *,
                        DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date,
                        ROW_NUMBER() OVER (PARTITION BY AREA_CD, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq
                FROM DE4_PROJECT3.TEMP_BRONZE_LIVE_CMRCL_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_LIVE_CMRCL_STTS""")

        # LIVE_PPLTN_STTS
        logging.info(f'REMOVE DUPLICATE DATA : LIVE_PPLTN_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_LIVE_PPLTN_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_LIVE_PPLTN_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_LIVE_PPLTN_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_LIVE_PPLTN_STTS AS 
                (SELECT AREA_NM, AREA_CD, AREA_CONGEST_LVL, AREA_CONGEST_MSG, AREA_PPLTN_MIN, AREA_PPLTN_MAX, 
                        MALE_PPLTN_RATE, FEMALE_PPLTN_RATE, PPLTN_RATE_0, PPLTN_RATE_10, PPLTN_RATE_20, 
                        PPLTN_RATE_30, PPLTN_RATE_40, PPLTN_RATE_50, PPLTN_RATE_60, PPLTN_RATE_70, 
                        RESNT_PPLTN_RATE, NON_RESNT_PPLTN_RATE, REPLACE_YN, PPLTN_TIME, FCST_YN, 
                        FCST_PPLTN, CreateAt
            FROM (
                SELECT *,
                        DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date,
                        ROW_NUMBER() OVER (PARTITION BY AREA_CD, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq
                FROM DE4_PROJECT3.TEMP_BRONZE_LIVE_PPLTN_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_LIVE_PPLTN_STTS""")

        # PRK_STTS
        logging.info(f'REMOVE DUPLICATE DATA : PRK_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_PRK_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_PRK_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_PRK_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_PRK_STTS AS 
                (SELECT PRK_NM, PRK_CD, PRK_TYPE, CPCTY, CUR_PRK_CNT, CUR_PRK_TIME, CUR_PRK_YN, PAY_YN, RATES, 
                TIME_RATES, ADD_RATES, ADD_TIME_RATES, ADDRESS, ROAD_ADDR, LNG, LAT, AREA_CD, CreateAt
            FROM (
                SELECT *,
                        DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date,
                        ROW_NUMBER() OVER (PARTITION BY PRK_CD, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq
                FROM DE4_PROJECT3.TEMP_BRONZE_PRK_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_PRK_STTS""")

        # ROAD_TRAFFIC_STTS
        logging.info(f'REMOVE DUPLICATE DATA : ROAD_TRAFFIC_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_ROAD_TRAFFIC_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_ROAD_TRAFFIC_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_ROAD_TRAFFIC_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_ROAD_TRAFFIC_STTS AS 
                (SELECT ROAD_TRAFFIC_STTS, `AVG_ROAD_DATA.ROAD_MSG`, `AVG_ROAD_DATA.ROAD_TRAFFIC_IDX`, 
                        `AVG_ROAD_DATA.ROAD_TRAFFIC_SPD`, `AVG_ROAD_DATA.ROAD_TRAFFIC_TIME`, 
                        AREA_CD, CreateAt
            FROM (
                SELECT *,
                        DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date,
                        ROW_NUMBER() OVER (PARTITION BY AREA_CD, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq
                FROM DE4_PROJECT3.TEMP_BRONZE_ROAD_TRAFFIC_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_ROAD_TRAFFIC_STTS""")

        # SBIKE_STTS
        logging.info(f'REMOVE DUPLICATE DATA : SBIKE_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_SBIKE_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_SBIKE_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_SBIKE_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_SBIKE_STTS AS 
                (SELECT SBIKE_SPOT_NM, SBIKE_SPOT_ID, SBIKE_SHARED, SBIKE_PARKING_CNT, SBIKE_RACK_CNT, 
                        SBIKE_X, SBIKE_Y, AREA_CD, CreateAt
            FROM (
                SELECT *,
                        DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date,
                        ROW_NUMBER() OVER (PARTITION BY SBIKE_SPOT_ID, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq
                FROM DE4_PROJECT3.TEMP_BRONZE_SBIKE_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_SBIKE_STTS""")

        # SUB_STTS
        logging.info(f'REMOVE DUPLICATE DATA : SUB_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_SUB_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_SUB_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_SUB_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_SUB_STTS AS 
                (SELECT SUB_STN_NM, SUB_STN_LINE, SUB_STN_RADDR, SUB_STN_JIBUN, SUB_STN_X, SUB_STN_Y, 
                        SUB_DETAIL, SUB_FACIINFO, AREA_CD, CreateAt
            FROM (
                SELECT *,
                        DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date,
                        ROW_NUMBER() OVER (PARTITION BY SUB_STN_NM, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq
                FROM DE4_PROJECT3.TEMP_BRONZE_SUB_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_SUB_STTS""")

        # WEATHER_STTS
        logging.info(f'REMOVE DUPLICATE DATA : WEATHER_STTS')
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.TEMP_BRONZE_WEATHER_STTS AS 
                (SELECT * FROM DE4_PROJECT3.BRONZE_WEATHER_STTS)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.BRONZE_WEATHER_STTS""")
        conn.execute("""
            CREATE TABLE DE4_PROJECT3.BRONZE_WEATHER_STTS AS 
                (SELECT WEATHER_TIME, TEMP, SENSIBLE_TEMP, MAX_TEMP, MIN_TEMP, HUMIDITY, WIND_DIRCT, WIND_SPD, 
                        PRECIPITATION, PRECPT_TYPE, PCP_MSG, SUNRISE, SUNSET, UV_INDEX_LVL, UV_INDEX, UV_MSG, 
                        PM25_INDEX, PM25, PM10_INDEX, PM10, AIR_IDX, AIR_IDX_MVL, AIR_IDX_MAIN, AIR_MSG, 
                        FCST24HOURS, NEWS_LIST, AREA_CD, CreateAt
            FROM (
                SELECT *,
                        DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') date,
                        ROW_NUMBER() OVER (PARTITION BY AREA_CD, DATE_FORMAT(CreateAt, '%%Y%%m%%d%%H') ORDER BY CreateAt) seq
                FROM DE4_PROJECT3.TEMP_BRONZE_WEATHER_STTS) first WHERE seq = 1)
        """)
        conn.execute("""DROP TABLE DE4_PROJECT3.TEMP_BRONZE_WEATHER_STTS""")




