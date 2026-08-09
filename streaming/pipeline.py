import json
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.pvalue import TaggedOutput
from dotenv import load_dotenv

from streaming.schemas import (
    REJECTED_SCHEMA,
    VALID_FIELDS,
    VALID_SCHEMA,
)


TIME_FORMAT = "%Y-%m-%d %H:%M:%S UTC"


def now():
    return datetime.now(timezone.utc).strftime(TIME_FORMAT)


def rejected(message, raw, reason, event=None):
    event = event or {}

    publish_time = message.publish_time
    if hasattr(publish_time, "ToDatetime"):
        publish_time = publish_time.ToDatetime(
            tzinfo=timezone.utc
        )
    if isinstance(publish_time, datetime):
        publish_time = publish_time.strftime(TIME_FORMAT)

    return {
        "event_id": event.get("event_id"),
        "event_time": event.get("event_time"),
        "ingestion_time": event.get("ingestion_time"),
        "message_id": message.message_id,
        "publish_time": publish_time,
        "raw_payload": raw,
        "error_type": "validation_error",
        "error_reason": reason,
        "pipeline_timestamp": now(),
    }


class SplitEvent(beam.DoFn):
    INVALID = "invalid"

    def process(self, message):
        raw = message.data.decode("utf-8", errors="replace")

        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            yield TaggedOutput(
                self.INVALID,
                rejected(message, raw, "JSON tidak valid"),
            )
            return

        missing = [
            field
            for field in VALID_FIELDS
            if event.get(field) is None
        ]

        positive = all(
            event.get(field, 0) > 0
            for field in (
                "trip_distance",
                "fare_amount",
                "total_amount",
            )
        )

        if missing or not positive:
            reason = (
                "Field kosong: " + ", ".join(missing)
                if missing
                else "Nilai perjalanan harus lebih besar dari 0"
            )
            yield TaggedOutput(
                self.INVALID,
                rejected(message, raw, reason, event),
            )
            return

        yield event


def options():
    load_dotenv()

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).runner = (
        "DataflowRunner"
    )
    pipeline_options.view_as(StandardOptions).streaming = True

    google = pipeline_options.view_as(GoogleCloudOptions)
    google.project = os.getenv("GCP_PROJECT_ID")
    google.region = os.getenv("DATAFLOW_REGION")
    google.job_name = os.getenv("DATAFLOW_JOB_NAME")
    google.staging_location = os.getenv(
        "DATAFLOW_STAGING_LOCATION"
    )
    google.temp_location = os.getenv("DATAFLOW_TEMP_LOCATION")

    setup = pipeline_options.view_as(SetupOptions)
    setup.save_main_session = True
    setup.requirements_file = "requirements-streaming.txt"

    return pipeline_options


def run():
    load_dotenv()

    project = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_STAGING_DATASET")
    subscription = (
        f"projects/{project}/subscriptions/"
        f"{os.getenv('PUBSUB_SUBSCRIPTION')}"
    )
    valid_table = (
        f"{project}:{dataset}."
        f"{os.getenv('BQ_STREAM_TABLE')}"
    )
    rejected_table = (
        f"{project}:{dataset}."
        f"{os.getenv('BQ_REJECTED_STREAM_TABLE')}"
    )

    with beam.Pipeline(options=options()) as pipeline:
        result = (
            pipeline
            | "Read PubSub"
            >> ReadFromPubSub(
                subscription=subscription,
                with_attributes=True,
                id_label="event_id",
            )
            | "Split valid-invalid"
            >> beam.ParDo(SplitEvent()).with_outputs(
                SplitEvent.INVALID,
                main="valid",
            )
        )

        result.valid | "Write valid" >> WriteToBigQuery(
            valid_table,
            schema=VALID_SCHEMA,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
            with_auto_sharding=True,
        )

        result.invalid | "Write invalid" >> WriteToBigQuery(
            rejected_table,
            schema=REJECTED_SCHEMA,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
            with_auto_sharding=True,
        )


if __name__ == "__main__":
    run()