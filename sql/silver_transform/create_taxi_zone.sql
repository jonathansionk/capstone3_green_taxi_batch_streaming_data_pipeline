CREATE OR REPLACE TABLE
  `jcdeah-009.cp3_jonathan_silver_transform.taxi_zone`

AS

SELECT DISTINCT
  SAFE_CAST(LocationID AS INT64) AS location_id,

  TRIM(
    SAFE_CAST(Borough AS STRING)
  ) AS borough,

  TRIM(
    SAFE_CAST(Zone AS STRING)
  ) AS zone_name,

  TRIM(
    SAFE_CAST(service_zone AS STRING)
  ) AS service_zone

FROM
  `jcdeah-009.cp3_jonathan_staging.stg_taxi_zone`

WHERE
  LocationID IS NOT NULL;