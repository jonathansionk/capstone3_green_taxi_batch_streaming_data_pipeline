import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from google.cloud import pubsub_v1


TIME_FORMAT = "%Y-%m-%d %H:%M:%S UTC"


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--rate", type=float, default=1)
    parser.add_argument("--invalid-every", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def format_time(value):
    return value.strftime(TIME_FORMAT)


def create_event(invalid=False):
    pickup = datetime(
        2026,
        random.choice([6, 7]),
        random.randint(1, 28),
        random.randint(0, 23),
        random.randint(0, 59),
        tzinfo=timezone.utc,
    )
    dropoff = pickup + timedelta(minutes=random.randint(5, 45))

    distance = round(random.uniform(0.5, 15), 2)
    fare = round(3 + distance * 2.5, 2)
    extra = random.choice([0.0, 1.0])
    mta_tax = 0.5
    tip = round(fare * random.choice([0, 0.1, 0.15]), 2)
    tolls = random.choice([0.0, 0.0, 6.55])
    improvement = 1.0
    congestion = random.choice([0.0, 2.75])
    cbd_fee = random.choice([0.0, 0.75])
    total = round(
        fare
        + extra
        + mta_tax
        + tip
        + tolls
        + improvement
        + congestion
        + cbd_fee,
        2,
    )

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": format_time(dropoff),
        "ingestion_time": format_time(datetime.now(timezone.utc)),
        "schema_version": "1.0",
        "publisher_name": "jonathan",
        "source_type": "stream",
        "vendor_id": random.choice([1, 2]),
        "pickup_datetime": format_time(pickup),
        "dropoff_datetime": format_time(dropoff),
        "store_and_fwd_flag": "N",
        "rate_code_id": 1,
        "pickup_location_id": random.randint(1, 265),
        "dropoff_location_id": random.randint(1, 265),
        "passenger_count": random.randint(1, 4),
        "trip_distance": -3.0 if invalid else distance,
        "fare_amount": fare,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip,
        "tolls_amount": tolls,
        "improvement_surcharge": improvement,
        "total_amount": total,
        "payment_type": random.choice([1, 2]),
        "trip_type": 1,
        "congestion_surcharge": congestion,
        "cbd_congestion_fee": cbd_fee,
    }


def main():
    load_dotenv()
    args = arguments()

    project = os.getenv("GCP_PROJECT_ID")
    topic = os.getenv("PUBSUB_TOPIC")

    if not project or not topic:
        raise RuntimeError(
            "GCP_PROJECT_ID dan PUBSUB_TOPIC harus ada di .env"
        )

    publisher = None
    topic_path = None

    if not args.dry_run:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project, topic)

    try:
        for number in range(1, args.count + 1):
            invalid = (
                args.invalid_every > 0
                and number % args.invalid_every == 0
            )
            event = create_event(invalid)
            payload = json.dumps(event).encode("utf-8")

            if args.dry_run:
                message_id = "DRY-RUN"
                print(json.dumps(event, indent=2))
            else:
                message_id = publisher.publish(
                    topic_path,
                    payload,
                    event_id=event["event_id"],
                ).result(timeout=60)

            print(
                f"Event {number} | message_id={message_id} "
                f"| invalid={invalid}"
            )
            time.sleep(1 / args.rate)

    except KeyboardInterrupt:
        print("Publisher dihentikan")

    finally:
        if publisher:
            publisher.stop()


if __name__ == "__main__":
    main()