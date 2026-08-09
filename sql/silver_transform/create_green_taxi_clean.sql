CREATE OR REPLACE TABLE
  `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_clean`

PARTITION BY DATE (pickup_datetime)

CLUSTER BY
  source_type,
  pickup_location_id,
  dropoff_location_id

AS

WITH join_all_data AS (
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
    trip_duration_minutes,
    source_type,
    event_id,
    silver_loaded_at
  FROM
    `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_batch_clean`
  
  UNION ALL
  
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
    trip_duration_minutes,
    source_type,
    event_id,
    silver_loaded_at
  FROM 
    `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_stream_clean`
  
)

SELECT
  trip.vendor_id,
  
  CASE trip.vendor_id
    WHEN 1 THEN 'Creative Mobile Technologies, LLC'
    WHEN 2 THEN 'VeriFone Inc.'
    WHEN 6 THEN 'Myle Technologies Inc'
    ELSE 'Unknown'
  END AS vendor_name,

  trip.pickup_datetime,
  DATE(trip.pickup_datetime) AS pickup_date,
  FORMAT_TIMESTAMP('%A', trip.pickup_datetime) AS pickup_day_name,
  EXTRACT(DAYOFWEEK FROM trip.pickup_datetime) IN (1,7) AS is_weekend,
  EXTRACT(HOUR FROM trip.pickup_datetime) AS pickup_hour,
  
  CASE 
    WHEN EXTRACT(HOUR FROM trip.pickup_datetime) BETWEEN 7 AND 9 THEN 'morning rush'
    WHEN EXTRACT(HOUR FROM trip.pickup_datetime) BETWEEN 0 AND 5 THEN 'late night'
    WHEN EXTRACT(HOUR FROM trip.pickup_datetime) BETWEEN 6 AND 10 THEN 'morning'
    WHEN EXTRACT(HOUR FROM trip.pickup_datetime) BETWEEN 11 AND 15 THEN 'afternoon'
    WHEN EXTRACT(HOUR FROM trip.pickup_datetime) BETWEEN 16 AND 19 THEN 'evening rush'
    WHEN EXTRACT(HOUR FROM trip.pickup_datetime) BETWEEN 20 AND 23 THEN 'night'
    ELSE 'Unknown'
  END AS time_period,

  trip.dropoff_datetime,
  trip.store_and_fwd_flag,

  CASE 
    WHEN trip.store_and_fwd_flag = 'Y' THEN 'store and forward'
    WHEN trip.store_and_fwd_flag = 'N' THEN 'normal'
    ELSE 'Unknown'
  END AS store_and_fwd_name, 
  
  COALESCE(trip.rate_code_id, 99) AS rate_code_id,

  CASE rate_code_id
    WHEN 1 THEN 'standard rate'
    WHEN 2 THEN 'JFK'
    WHEN 3 THEN 'Newark'
    WHEN 4 THEN 'Nassau or Westchester'
    WHEN 5 THEN 'Negotiated fare'
    WHEN 6 THEN 'Group ride'
    WHEN 99 THEN 'Null/unknown'
    ELSE 'Unknown'
  END AS rate_code_name,

  trip.pickup_location_id,
  pickup_zone.borough AS pickup_borough,
  pickup_zone.zone_name AS pickup_zone,
  pickup_zone.service_zone AS pickup_service_zone,
 
  trip.dropoff_location_id,
  dropoff_zone.borough AS dropoff_borough,
  dropoff_zone.zone_name AS dropoff_zone,
  dropoff_zone.service_zone AS dropoff_service_zone,

  trip.passenger_count,
  trip.trip_distance,
  trip.trip_duration_minutes,

  trip.fare_amount,
  trip.extra,
  trip.mta_tax,
  trip.tip_amount,
  trip.tolls_amount,
  trip.improvement_surcharge,
  trip.congestion_surcharge,
  trip.cbd_congestion_fee,
  trip.total_amount,

  COALESCE(trip.payment_type, 5) AS payment_type,

  CASE payment_type
    WHEN 0 THEN 'flex fare frip'
    WHEN 1 THEN 'credit card'
    WHEN 2 THEN 'cash'
    WHEN 3 THEN 'no charge'
    WHEN 4 THEN 'dispute'
    WHEN 5 THEN 'unknown'
    WHEN 6 THEN 'voided trip'
    ELSE 'Unknown'
  END AS payment_type_name,

  trip.trip_type,

  CASE trip_type
    WHEN 1 THEN 'street-hail'
    WHEN 2 THEN 'dispatch'
    ELSE 'Unknown'
  END AS trip_name,

  trip.source_type,
  trip.event_id,
  trip.silver_loaded_at
FROM 
  join_all_data AS trip

LEFT JOIN
  `jcdeah-009.cp3_jonathan_silver_transform.taxi_zone` 
  AS pickup_zone
ON
  trip.pickup_location_id = pickup_zone.location_id

LEFT JOIN 
  `jcdeah-009.cp3_jonathan_silver_transform.taxi_zone`
  AS dropoff_zone
ON
  trip.dropoff_location_id = dropoff_zone.location_id


WHERE
  pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
  AND dropoff_datetime > pickup_datetime
  AND pickup_datetime >= TIMESTAMP('2026-04-01')
  AND pickup_datetime < TIMESTAMP('2026-08-01')
  AND trip_distance > 0
  AND total_amount > 0
  AND fare_amount > 0

  AND TIMESTAMP_DIFF(dropoff_datetime,pickup_datetime,SECOND) BETWEEN 300 AND 14400

  
















