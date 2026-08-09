CREATE OR REPLACE VIEW
    `jcdeah-009.cp3_jonathan_gold_mart.vw_daily_trip_summary`
AS

SELECT

    pickup_date,
    pickup_day_name,
    is_weekend,
    pickup_hour,
    time_period,
    trip_duration_minutes,

    COUNT(pickup_datetime) AS total_trips,
    ROUND(AVG(trip_duration_minutes),2) AS average_duration_minutes,
    ROUND(AVG(trip_distance),2) AS average_trip_distance,


    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount),2) AS average_revenue,
    ROUND(AVG(fare_amount), 2)AS average_fare,
    ROUND(AVG(tip_amount),2) AS average_tip,
    source_type

FROM 
    `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_clean`

GROUP BY
    pickup_date,
    pickup_day_name,
    is_weekend,
    pickup_hour,
    time_period,
    trip_duration_minutes,
    source_type

ORDER BY
  pickup_date
