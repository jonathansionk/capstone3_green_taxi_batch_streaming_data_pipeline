
-- Mengecek apakah ada durasi yang invalid (5 menit - 4 jam)
SELECT
  source_type,

  COUNT(*) AS total_rows,

  COUNTIF(
    trip_duration_minutes <= 0
  ) AS duration_zero_or_negative,

  COUNTIF(
    trip_duration_minutes > 240
  ) AS duration_over_240,

  MIN(
    trip_duration_minutes
  ) AS minimum_duration,

  MAX(
    trip_duration_minutes
  ) AS maximum_duration

FROM
  `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_clean`

GROUP BY
  source_type

ORDER BY
  source_type;

-- validasi jumlah data bacth dan stream

SELECT
  EXTRACT(YEAR FROM pickup_datetime) AS trip_year,
  EXTRACT(MONTH FROM pickup_datetime) AS trip_month,
  source_type,
  COUNT(pickup_datetime) AS total_trips
FROM 
  `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_clean`
GROUP BY
  trip_year,
  trip_month,
  source_type

ORDER BY
  trip_year, trip_month ASC
