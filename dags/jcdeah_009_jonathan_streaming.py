import os

import pendulum
from airflow.sdk import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)


PROJECT_ID = os.environ["GCP_PROJECT_ID"]
LOCATION = os.environ["GCP_LOCATION"]
CONN_ID = os.getenv(
    "AIRFLOW_GCP_CONN_ID",
    "google_cloud_default",
)


def sql_task(task_id, sql_file):
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": f"{{% include '{sql_file}' %}}",
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=CONN_ID,
    )


with DAG(
    dag_id="jcdeah_009_jonathan_streaming",
    description="Membuat staging dataset dan tabel streaming kosong",
    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="Asia/Jakarta",
    ),
    schedule=None,
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
    tags=["jcdeah_009", "streaming", "bootstrap"],
) as dag:

    create_staging_dataset = sql_task(
        "create_staging_dataset",
        "staging/create_staging.sql",
    )

    create_stream_tables = sql_task(
        "create_stream_tables",
        "streaming/create_stream_tables.sql",
    )

    create_staging_dataset >> create_stream_tables