{{ config(
    materialized='table'
) }}

WITH event_data AS (
    SELECT
        AREA_CD,
        EVENT_NM,
        EVENT_PERIOD,
        EVENT_PLACE,
        CreateAt      
    FROM {{ source('DE4_PROJECT3', 'BRONZE_EVENT_STTS') }}
    WHERE AREA_CD IS NOT NULL
      AND CreateAt = (
          SELECT MAX(CreateAt)
          FROM {{ source('DE4_PROJECT3', 'BRONZE_EVENT_STTS') }}
          WHERE AREA_CD = BRONZE_EVENT_STTS.AREA_CD
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
    event_data.AREA_CD,
    event_data.EVENT_NM,
    event_data.EVENT_PERIOD,
    event_data.EVENT_PLACE,
    population_data.AREA_CONGEST_LVL,
    population_data.AREA_PPLTN_MIN,
    population_data.AREA_PPLTN_MAX
FROM event_data
LEFT JOIN population_data
ON event_data.AREA_CD = population_data.AREA_CD
