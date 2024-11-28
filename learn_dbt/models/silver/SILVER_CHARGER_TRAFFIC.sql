{{ config(
    materialized='table'
) }}

WITH chargers AS (
    SELECT
        AREA_CD,
        STAT_ID,
        STAT_USETIME,
        CreateAt
    FROM {{ source('DE4_PROJECT3', 'BRONZE_CHARGER_STTS') }}
    WHERE AREA_CD IS NOT NULL
      AND CreateAt = (
          SELECT MAX(CreateAt)
          FROM {{ source('DE4_PROJECT3', 'BRONZE_CHARGER_STTS') }}
          WHERE AREA_CD = BRONZE_CHARGER_STTS.AREA_CD
      )
),
traffic AS (
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
)
SELECT DISTINCT
    chargers.AREA_CD AS AREA_CD,
    chargers.STAT_ID AS CHARGER_ID,
    chargers.STAT_USETIME AS USE_TIME,
    traffic.ROAD_TRAFFIC_SPD AS ROAD_SPEED,
    traffic.ROAD_TRAFFIC_TIME AS TRAFFIC_TIME
FROM chargers
LEFT JOIN traffic
ON chargers.AREA_CD = traffic.AREA_CD
