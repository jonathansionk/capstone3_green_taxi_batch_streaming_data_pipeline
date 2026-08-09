CREATE OR REPLACE TABLE
  `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_stream_clean`

PARTITION BY DATE (pickup_datetime)

CLUSTER BY
  pickup_location_id,
  dropoff_location_id

  AS

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

    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE ) AS trip_duration_minutes,

    'stream' AS source_type,
    event_id,
    CURRENT_TIMESTAMP() AS silver_loaded_at
     
    FROM
    `jcdeah-009.cp3_jonathan_staging.stg_green_taxi_stream`
    
    WHERE 
      pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND dropoff_datetime > pickup_datetime
      AND pickup_datetime >= (TIMESTAMP('2026-06-01'))
      AND pickup_datetime < (TIMESTAMP('2026-08-01'))
      AND trip_distance > 0
      AND fare_amount > 0
      AND total_amount > 0
  
  
