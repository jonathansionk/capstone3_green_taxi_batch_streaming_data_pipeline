import os

import pendulum
from airflow.sdk import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


# ============================================================
# KONFIGURASI ENIRONMENT
# ============================================================

def env(name, default=None):
    value = os.getenv(name, default)

    if not value:
        raise ValueError(f"Environment variable {name} belum diisi")

    return value


PROJECT_ID = env("GCP_PROJECT_ID")
LOCATION = env("GCP_LOCATION")
CONN_ID = env("AIRFLOW_GCP_CONN_ID", "google_cloud_default")

BUCKET = env("GCS_BUCKET")
STAGING_DATASET = env("BQ_STAGING_DATASET")
SILVER_DATASET = env("BQ_SILVER_DATASET")
GOLD_DATASET = env("BQ_GOLD_DATASET")

STAGING = f"{PROJECT_ID}.{STAGING_DATASET}"
SILVER = f"{PROJECT_ID}.{SILVER_DATASET}"
GOLD = f"{PROJECT_ID}.{GOLD_DATASET}"

YEAR = env("BATCH_YEAR")
APRIL = env("BATCH_MONTH_APRIL")
MAY = env("BATCH_MONTH_MAY")
TABLE_PREFIX = env("BQ_BATCH_TABLE_PREFIX")

APRIL_TABLE = f"{STAGING}.{TABLE_PREFIX}_{YEAR}_{APRIL}"
MAY_TABLE = f"{STAGING}.{TABLE_PREFIX}_{YEAR}_{MAY}"

TAXI_ZONE_TABLE = (f"{STAGING}.{env('BQ_TAXI_ZONE_TABLE')}")

STREAM_TABLE = (f"{STAGING}.{env('BQ_STREAM_TABLE')}")

SILVER_CLEAN = f"{SILVER}.green_taxi_clean"

DAG_ID = env("AIRFLOW_END_TO_END_DAG_ID")

BQ_OPTIONS = {
    "project_id": PROJECT_ID,
    "location": LOCATION,
    "gcp_conn_id": CONN_ID,
}


# ============================================================
# HELPER TASK
# ============================================================

def sql_task(task_id, sql_file):
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": f"{{% include '{sql_file}' %}}",
                "useLegacySql": False,
            }
        },
        **BQ_OPTIONS,
    )


def load_task(
    task_id,
    source_object,
    destination_table,
    source_format,
    **kwargs,
):
    return GCSToBigQueryOperator(
        task_id=task_id,
        bucket=BUCKET,
        source_objects=[source_object],
        destination_project_dataset_table=destination_table,
        source_format=source_format,
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        **BQ_OPTIONS,
        **kwargs,
    )


def check_task(task_id, sql):
    return BigQueryCheckOperator(
        task_id=task_id,
        sql=sql,
        use_legacy_sql=False,
        **BQ_OPTIONS,
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=DAG_ID,
    description="Green Taxi end-to-end Architecture",
    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="Asia/Jakarta",
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    tags=["jcdeah_009", "capstone_3", "end_to_end"],
) as dag:

    # --------------------------------------------------------
    # STAGING
    # --------------------------------------------------------

    create_staging = sql_task(
        "create_staging_dataset",
        "staging/create_staging.sql",
    )

    load_april = load_task(
        "load_april",
        env("GCS_BATCH_APRIL_OBJECT"),
        APRIL_TABLE,
        "PARQUET",
    )

    load_may = load_task(
        "load_may",
        env("GCS_BATCH_MAY_OBJECT"),
        MAY_TABLE,
        "PARQUET",
    )

    load_taxi_zone = load_task(
        "load_taxi_zone",
        env("GCS_TAXI_ZONE_OBJECT"),
        TAXI_ZONE_TABLE,
        "CSV",
        skip_leading_rows=1,
        field_delimiter=",",
        encoding="UTF-8",
    )

    check_staging = check_task(
        "check_staging_ready",
        f"""
        SELECT
          (SELECT COUNT(*) > 0 FROM `{APRIL_TABLE}`),
          (SELECT COUNT(*) > 0 FROM `{MAY_TABLE}`),
          (SELECT COUNT(*) > 0 FROM `{TAXI_ZONE_TABLE}`),
          (SELECT COUNT(*) > 0 FROM `{STREAM_TABLE}`)
        """,
    )

    # --------------------------------------------------------
    # SILVER
    # --------------------------------------------------------

    create_silver = sql_task(
        "create_silver_dataset",
        "silver_transform/create_schema_silver.sql",
    )

    create_batch_clean = sql_task(
        "create_batch_clean",
        "batch/create_batch_clean_tables.sql",
    )

    create_stream_clean = sql_task(
        "create_stream_clean",
        "streaming/create_stream_clean_table.sql",
    )

    create_taxi_zone = sql_task(
        "create_silver_taxi_zone",
        "silver_transform/create_taxi_zone.sql",
    )

    create_green_taxi_clean = sql_task(
        "create_green_taxi_clean",
        "silver_transform/create_green_taxi_clean.sql",
    )

    check_silver = check_task(
        "check_silver",
        """
        {% include
        'silver_transform/check_green_taxi_clean.sql'
        %}
        """,
    )

    # --------------------------------------------------------
    # GOLD
    # --------------------------------------------------------

    create_gold = sql_task(
        "create_gold_dataset",
        "gold_mart/create_schema_gold.sql",
    )

    create_daily_view = sql_task(
        "create_daily_trip_summary",
        "gold_mart/vw_daily_trip_summary.sql",
    )

    create_payment_view = sql_task(
        "create_payment_summary",
        "gold_mart/vw_payment_summary.sql",
    )

    create_zone_view = sql_task(
        "create_zone_performance",
        "gold_mart/vw_zone_performance.sql",
    )

    check_gold = check_task(
        "check_gold",
        f"""
        WITH silver AS (
          SELECT COUNT(*) AS total
          FROM `{SILVER_CLEAN}`
        )

        SELECT
          total = (
            SELECT SUM(total_trips)
            FROM `{GOLD}.vw_daily_trip_summary`
          )

          AND total = (
            SELECT SUM(total_trips)
            FROM `{GOLD}.vw_payment_summary`
          )

          AND total = (
            SELECT SUM(total_trips)
            FROM `{GOLD}.vw_zone_performance`
          )

        FROM silver
        """,
    )

    # --------------------------------------------------------
    # DEPENDENCY
    # --------------------------------------------------------

    # STAGING
    create_staging >> [
        load_april,
        load_may,
        load_taxi_zone
    ] >> check_staging >> create_silver

    # SILVER
    create_silver >> [
        create_batch_clean,
        create_stream_clean,
        create_taxi_zone,
    ] >> create_green_taxi_clean >> check_silver >> create_gold

    # GOLD
    create_gold >> [
        create_daily_view,
        create_payment_view,
        create_zone_view,
    ] >> check_gold