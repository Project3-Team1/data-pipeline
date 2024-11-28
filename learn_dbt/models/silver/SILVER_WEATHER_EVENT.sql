{{ config(
    materialized='table'
) }}

WITH weather AS (
    SELECT
        AREA_CD,
        TEMP,
        WIND_SPD,
        CreateAt
    FROM {{ source('DE4_PROJECT3', 'BRONZE_WEATHER_STTS') }}
    WHERE AREA_CD IS NOT NULL
      AND CreateAt = (
          SELECT MAX(CreateAt)
          FROM {{ source('DE4_PROJECT3', 'BRONZE_WEATHER_STTS') }}
          WHERE AREA_CD = BRONZE_WEATHER_STTS.AREA_CD
      )
),
event_table AS (
    SELECT
        AREA_CD,
        EVENT_NM AS EVENT_COUNT,
        CreateAt
    FROM {{ source('DE4_PROJECT3', 'BRONZE_EVENT_STTS') }}
    WHERE AREA_CD IS NOT NULL
      AND CreateAt = (
          SELECT MAX(CreateAt)
          FROM {{ source('DE4_PROJECT3', 'BRONZE_EVENT_STTS') }}
          WHERE AREA_CD = BRONZE_EVENT_STTS.AREA_CD
      )
)
SELECT DISTINCT
    weather.AREA_CD,
    weather.TEMP,
    weather.WIND_SPD,
    event_table.EVENT_COUNT
FROM weather
LEFT JOIN event_table
ON weather.AREA_CD = event_table.AREA_CD
