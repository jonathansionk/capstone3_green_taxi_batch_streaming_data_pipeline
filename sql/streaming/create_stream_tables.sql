-- ==========================================================
-- VALID GREEN TAXI STREAMING EVENTS
-- ==========================================================

CREATE TABLE IF NOT EXISTS
    `jcdeah-009.cp3_jonathan_staging.stg_green_taxi_stream`
(
    -- Metadata event
    event_id STRING,
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    schema_version STRING,
    publisher_name STRING,
    source_type STRING,

    -- Data perjalanan
    vendor_id INT64,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    rate_code_id INT64,
    pickup_location_id INT64,
    dropoff_location_id INT64,
    passenger_count INT64,
    trip_distance FLOAT64,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    improvement_surcharge FLOAT64,
    total_amount FLOAT64,
    payment_type INT64,
    trip_type INT64,
    congestion_surcharge FLOAT64,
    cbd_congestion_fee FLOAT64
)

PARTITION BY DATE(event_time)

CLUSTER BY
    pickup_location_id,
    dropoff_location_id

OPTIONS (
    description = "Raw valid Green Taxi streaming events received from Pub/Sub"
);


-- ==========================================================
-- REJECTED GREEN TAXI STREAMING EVENTS
-- ==========================================================

CREATE TABLE IF NOT EXISTS
    `jcdeah-009.cp3_jonathan_staging.rejected_stream_events`
(
    -- Event metadata yang masih dapat dibaca
    event_id STRING,
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP,

    -- Metadata Pub/Sub
    message_id STRING,
    publish_time TIMESTAMP,

    -- Informasi error
    raw_payload STRING,
    error_type STRING,
    error_reason STRING,

    -- Waktu event diproses oleh pipeline
    pipeline_timestamp TIMESTAMP
)

PARTITION BY DATE(pipeline_timestamp)

CLUSTER BY error_type

OPTIONS (
    description = "Green Taxi events rejected during parsing or validation"
);