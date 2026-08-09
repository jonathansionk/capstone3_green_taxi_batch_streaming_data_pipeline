CREATE OR REPLACE VIEW
    `jcdeah-009.cp3_jonathan_gold_mart.vw_payment_summary`
AS

SELECT
    payment_type_name,
    vendor_name,
    pickup_zone,
    dropoff_zone,

    COUNT(pickup_datetime) AS total_trips,

    ROUND(SUM(total_amount),2) AS total_revenue,
    ROUND(AVG(total_amount),2) AS average_revenue,
    ROUND(SUM(tip_amount),2) AS total_tip,
    ROUND(AVG(tip_amount),2) AS average_tip_amount,
    ROUND(AVG(fare_amount),2) AS average_fare_amount,

    source_type

FROM 
    `jcdeah-009.cp3_jonathan_silver_transform.green_taxi_clean`
GROUP BY 
    payment_type_name,
    vendor_name,
    pickup_zone,
    dropoff_zone,
    source_type
ORDER BY
  total_trips DESC


