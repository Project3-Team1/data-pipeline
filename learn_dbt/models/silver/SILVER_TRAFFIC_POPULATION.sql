{{ config(
    materialized='table'
) }}

WITH traffic AS (
    SELECT
        AREA_CD,
        `AVG_ROAD_DATA.ROAD_TRAFFIC_SPD` AS ROAD_TRAFFIC_SPD,
        `AVG_ROAD_DATA.ROAD_TRAFFIC_TIME` AS ROAD_TRAFFIC_TIME,
        CreateAt
    FROM {{ source('DE4_PROJECT3', 'BRONZE_ROAD_TRAFFIC_STTS') }}
    WHERE AREA_CD IS NOT NULL
      AND CreateAt = (
          SELECT MAX(CreateAt)
          FROM {{ source('DE4_PROJECT3', 'BRONZE_ROAD_TRAFFIC_STTS') }}
          WHERE AREA_CD = BRONZE_ROAD_TRAFFIC_STTS.AREA_CD
      )
),
population_data AS (
    SELECT
        AREA_CD,
        AREA_CONGEST_LVL,
        AREA_PPLTN_MIN,
        AREA_PPLTN_MAX,
        CreateAt
    FROM {{ source('DE4_PROJECT3', 'BRONZE_LIVE_PPLTN_STTS') }}
    WHERE AREA_CD IS NOT NULL
      AND CreateAt = (
          SELECT MAX(CreateAt)
          FROM {{ source('DE4_PROJECT3', 'BRONZE_LIVE_PPLTN_STTS') }}
          WHERE AREA_CD = BRONZE_LIVE_PPLTN_STTS.AREA_CD
      )
)
SELECT DISTINCT
    traffic.AREA_CD AS AREA_CD,
    traffic.ROAD_TRAFFIC_SPD AS ROAD_TRAFFIC_SPD,
    traffic.ROAD_TRAFFIC_TIME AS ROAD_TRAFFIC_TIME,
    CASE 
        WHEN population_data.AREA_CONGEST_LVL = '여유' THEN 1
        WHEN population_data.AREA_CONGEST_LVL = '보통' THEN 2
        WHEN population_data.AREA_CONGEST_LVL = '약간 붐빔' THEN 3
        WHEN population_data.AREA_CONGEST_LVL = '붐빔' THEN 4
        ELSE NULL  -- 예외 처리
    END AS AREA_CONGEST_LVL, -- 변환된 값
    population_data.AREA_PPLTN_MIN AS AREA_PPLTN_MIN,
    population_data.AREA_PPLTN_MAX AS AREA_PPLTN_MAX,
    'KR-11' AS Country_code -- 문자열 리터럴로 'KR-11' 사용
FROM traffic
LEFT JOIN population_data
ON traffic.AREA_CD = population_data.AREA_CD
