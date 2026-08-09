SELECT
  EXTRACT(YEAR FROM pickup_datetime) AS trip_year,
  EXTRACT(MONTH FROM pickup_datetime) AS trip_month,
  source_type,
  COUNT(pickup_datetime) AS trip_count
FROM
  `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_batch_clean`

GROUP BY
  trip_year,
  trip_month,
  source_type

ORDER BY
  trip_month


