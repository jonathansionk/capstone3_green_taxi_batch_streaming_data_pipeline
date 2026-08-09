-- =======================================================
-- Pickup Zone mana yang paling banyak perjalanan ?
-- =======================================================

SELECT 
    pickup_zone,

    SUM(total_trips) AS total_perjalanan,

    ROUND(SUM(total_revenue),2) AS total_revenue,

    ROUND(SUM(total_revenue) / SUM(total_trips),2) AS revenue_per_trip,
    source_type

FROM
    `jcdeah-009.cp3_jonathan_gold_mart.vw_zone_performance`
GROUP BY
    pickup_zone,
    source_type
ORDER BY
    total_perjalanan DESC

LIMIT 5



-- =======================================================
-- Kapan waktu perjalanan paling ramai ?
-- =======================================================

SELECT 
    time_period,
    ROUND(SUM(total_trips),2) AS total_perjalanan,
    ROUND(AVG(average_duration_minutes),2) AS average_trip_duration,
    ROUND(AVG(average_trip_distance),2) AS average_trip_distance,
    ROUND(SUM(total_revenue),2) AS total_revenue
FROM   
    `jcdeah-009.cp3_jonathan_gold_mart.vw_daily_trip_summary`
GROUP BY 
    time_period
ORDER BY
    total_perjalanan DESC
    


-- =======================================================
-- Metode Pembayaran apa yang paling banyak digunakan ?
-- =======================================================

WITH payment_summary AS (
SELECT
    payment_type_name,
    -- ROUND(COUNT(payment_type_name),2) AS total_tipe_pembayaran,
    ROUND(SUM(total_trips),2) AS total_perjalanan,
    ROUND(SUM(total_revenue),2) AS total_revenue,
    ROUND(SUM(total_tip),2) AS total_tip

FROM
    `jcdeah-009.cp3_jonathan_gold_mart.vw_payment_summary`
GROUP BY
    payment_type_name
)

SELECT
    
    payment_type_name,
    total_perjalanan,
    RANK() OVER(
        ORDER BY total_perjalanan DESC
    ) AS payment_rank,

    ROUND(total_perjalanan / SUM(total_perjalanan) OVER () * 100, 2) AS usage_percentage,
    total_revenue,
    total_tip
FROM payment_summary

ORDER BY 
    payment_rank





-- =======================================================
-- perbandingan data batch dan streaming
-- =======================================================

SELECT
    source_type,

    ROUND(SUM(total_trips),2) AS total_perjalanan,

    ROUND(SUM(total_revenue),2) AS total_revenue,

    ROUND(SUM(total_revenue)/SUM(total_trips),2) AS average_revenue_per_trip,

    ROUND(AVG(average_fare),2) AS average_fare,

    ROUND(AVG(average_tip),2) AS average_tip,

    ROUND(AVG(average_trip_distance),2) AS average_trip_distance
FROM
    `jcdeah-009.cp3_jonathan_gold_mart.vw_daily_trip_summary`
GROUP BY
    source_type
ORDER BY
    total_perjalanan DESC