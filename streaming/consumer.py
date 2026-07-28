from __future__ import annotations

import argparse
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

from data_platform.config import Settings

DDL = """
CREATE SCHEMA IF NOT EXISTS streaming;
CREATE TABLE IF NOT EXISTS streaming.trip_events_minute (
    event_minute timestamptz NOT NULL,
    pickup_zone_id integer NOT NULL,
    trip_count bigint NOT NULL,
    revenue_amount numeric NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_minute, pickup_zone_id)
);
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate replayed trips by minute and zone.")
    parser.add_argument("--bootstrap-servers", default="redpanda:9092")
    parser.add_argument("--topic", default="nyc-taxi-trips")
    parser.add_argument("--max-events", type=int, default=10000)
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id="nyc-taxi-minute-aggregator",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: __import__("json").loads(value.decode()),
    )
    settings = Settings()
    processed = 0
    with psycopg2.connect(settings.postgres_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(DDL)
            for message in consumer:
                event = message.value
                event_minute = datetime.fromisoformat(
                    str(event["tpep_pickup_datetime"])
                ).replace(second=0, microsecond=0)
                cursor.execute(
                    """
                    INSERT INTO streaming.trip_events_minute (
                        event_minute, pickup_zone_id, trip_count, revenue_amount
                    )
                    VALUES (%s, %s, 1, %s)
                    ON CONFLICT (event_minute, pickup_zone_id) DO UPDATE SET
                        trip_count = streaming.trip_events_minute.trip_count + 1,
                        revenue_amount = streaming.trip_events_minute.revenue_amount
                            + EXCLUDED.revenue_amount,
                        updated_at = now()
                    """,
                    (
                        event_minute,
                        int(event["PULocationID"]),
                        float(event["total_amount"]),
                    ),
                )
                processed += 1
                if processed % 500 == 0:
                    connection.commit()
                if processed >= args.max_events:
                    connection.commit()
                    print(f"Aggregated {processed} events")
                    return


if __name__ == "__main__":
    main()
