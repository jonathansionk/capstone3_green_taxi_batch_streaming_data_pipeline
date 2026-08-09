CREATE OR REPLACE VIEW
    `jcdeah-009.cp3_jonathan_gold_mart.vw_zone_performance`
AS

SELECT
    pickup_borough,
    COALESCE(pickup_zone,'unknown') AS pickup_zone,
    pickup_service_zone,

    dropoff_borough,
    COALESCE(dropoff_zone, 'unknown') AS dropoff_zone,
    dropoff_service_zone,
    
    COUNT(pickup_datetime) AS total_trips,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount),2) AS average_revenue,
    ROUND(AVG(fare_amount),2) AS average_fare_amount,
    ROUND(AVG(tip_amount),2) AS average_tip_amount,
    ROUND(AVG(trip_distance),2) AS average_trip_distance,
    ROUND(AVG(trip_duration_minutes),2) AS average_trip_duration,

    source_type

FROM 
    `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_clean`
GROUP BY
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    source_type

ORDER BY
    total_trips DESC

    