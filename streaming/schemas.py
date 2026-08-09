VALID_SCHEMA = (
    "event_id:STRING,event_time:TIMESTAMP,ingestion_time:TIMESTAMP,"
    "schema_version:STRING,publisher_name:STRING,source_type:STRING,"
    "vendor_id:INTEGER,pickup_datetime:TIMESTAMP,"
    "dropoff_datetime:TIMESTAMP,store_and_fwd_flag:STRING,"
    "rate_code_id:INTEGER,pickup_location_id:INTEGER,"
    "dropoff_location_id:INTEGER,passenger_count:INTEGER,"
    "trip_distance:FLOAT,fare_amount:FLOAT,extra:FLOAT,mta_tax:FLOAT,"
    "tip_amount:FLOAT,tolls_amount:FLOAT,"
    "improvement_surcharge:FLOAT,total_amount:FLOAT,"
    "payment_type:INTEGER,trip_type:INTEGER,"
    "congestion_surcharge:FLOAT,cbd_congestion_fee:FLOAT"
)

REJECTED_SCHEMA = (
    "event_id:STRING,event_time:TIMESTAMP,ingestion_time:TIMESTAMP,"
    "message_id:STRING,publish_time:TIMESTAMP,raw_payload:STRING,"
    "error_type:STRING,error_reason:STRING,"
    "pipeline_timestamp:TIMESTAMP"
)

VALID_FIELDS = tuple(
    field.split(":")[0]
    for field in VALID_SCHEMA.split(",")
)