CREATE OR REPLACE TABLE 
  `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_batch_clean`

  PARTITION BY DATE (pickup_datetime)

  CLUSTER BY
    pickup_location_id,
    dropoff_location_id
  
  AS

  WITH combined_batch AS (
    SELECT *
    FROM `jcdeah-009.cp3_jonathan_staging.stg_green_taxi_2026_04`

    UNION ALL

    SELECT *
    FROM `jcdeah-009.cp3_jonathan_staging.stg_green_taxi_2026_05`

  ),

-- untuk menyeragamkan tipe data & nama kolom
  normalized AS (
    SELECT
      SAFE_CAST(VendorID AS INT) AS vendor_id,
      SAFE_CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
      SAFE_CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
      SAFE_CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
      SAFE_CAST(RatecodeID AS INT) AS rate_code_id,
      SAFE_CAST(PULocationID AS INT) AS pickup_location_id,
      SAFE_CAST(DOLocationID AS INT) AS dropoff_location_id,
      SAFE_CAST(passenger_count AS INT) AS passenger_count,
      SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
      SAFE_CAST(fare_amount AS FLOAT64) AS fare_amount,
      SAFE_CAST(extra AS FLOAT64) AS extra,
      SAFE_CAST(mta_tax AS FLOAT64) AS mta_tax,
      SAFE_CAST(tip_amount AS FLOAT64) AS tip_amount,
      SAFE_CAST(tolls_amount AS FLOAT64) AS tolls_amount,
      SAFE_CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
      SAFE_CAST(total_amount AS FLOAT64) AS total_amount,
      SAFE_CAST(payment_type AS INT) AS payment_type,
      SAFE_CAST(trip_type AS INT) AS trip_type,
      SAFE_CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
      SAFE_CAST(cbd_congestion_fee AS FLOAT64) AS cbd_congestion_fee
    FROM combined_batch
   
  )

  SELECT 
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    cbd_congestion_fee,



    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_minutes,

    'batch' AS source_type,

    CAST(NULL AS STRING) AS event_id,

    CURRENT_TIMESTAMP() AS silver_loaded_at

  FROM normalized
  WHERE
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime > pickup_datetime
    AND trip_distance > 0
    AND fare_amount > 0
    AND total_amount > 0
    AND pickup_datetime >= TIMESTAMP('2026-04-01')
    AND pickup_datetime < TIMESTAMP('2026-06-01')
